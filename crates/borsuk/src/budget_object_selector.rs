//! Pure finite-model routing and membership diagnostics. Charges are modeled;
//! caller-supplied neighbor weights are not authenticated recall evidence.
//! No fitting, I/O, publication, mutation lifecycle, or quality qualification.

use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::mem::size_of;

pub const HIDDEN: usize = 64;
pub const MIXTURES: usize = 4;
pub const HEAD_TOP: usize = 8;
pub const MAX_CANDIDATES: usize = 256;
pub const MAX_SELECTED: usize = 15;
pub const MAX_READS: u64 = 32;
pub const MAX_BYTES: u64 = 16 * 1024 * 1024;
pub type Hash = [u8; 32];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid(&'static str),
    Overflow,
    /// Valid algorithm/resource refusal, distinct from malformed input.
    Refused(&'static str),
    IdentityMismatch,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Live Vec capacities plus declared fixed arrays; excludes borrowed
    /// storage, allocator overhead, and compiler/runtime stack overhead.
    pub transient_bytes: usize,
    /// Conservative modeled scalar work, including ranking and validation.
    pub operations: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Memory {
    pub peak_capacity_bytes: usize,
    pub retained_capacity_bytes: usize,
    /// Fixed evaluator arrays; excludes compiler/runtime stack overhead.
    pub fixed_array_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Work {
    pub operations: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Storage {
    pub borrowed_bytes: usize,
    /// Offline group-owner state; never part of a compact serving-root claim.
    pub offline_group_owner_bytes: usize,
    pub borrowed_directory_bytes: usize,
    /// Derived group/row/body-size table, also offline diagnostic state.
    pub owned_capacity_bytes: usize,
    pub validation_peak_capacity_bytes: usize,
    pub validation_operations: u64,
}

fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b).ok_or(Error::Overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b).ok_or(Error::Overflow)
}
fn sum(a: u64, b: u64) -> Result<u64, Error> {
    a.checked_add(b).ok_or(Error::Overflow)
}
fn geometry(d: usize, s: usize) -> Result<usize, Error> {
    if !(1..=768).contains(&d) || !(1..=335).contains(&s) {
        return Err(Error::Invalid("dimension/grid"));
    }
    mul(s, s)
}
pub fn parameter_count(d: usize, s: usize) -> Result<usize, Error> {
    geometry(d, s)?;
    add(
        add(mul(HIDDEN, add(d, 1)?)?, MIXTURES * 65)?,
        mul(8 * 65, s)?,
    )
}
/// Serving has a digest-only directory, so every occupied body, including a
/// short tail, reserves the complete maximum body before selection.
pub fn max_body_reservation_bytes(dimension: usize) -> Result<u64, Error> {
    geometry(dimension, 1)?;
    Ok(add(64, mul(1024, add(dimension, 12)?)?)? as u64)
}

// Comparison admissions use a deliberately conservative quadratic ceiling
// for <=256 candidates and a 64*n*ceil(log2(n)) model for stdlib larger sorts.
// These are modeled work units, not CPU instructions or timing claims.
fn sort_work(n: usize) -> Result<u64, Error> {
    let work = if n <= MAX_CANDIDATES {
        mul(n, n)?
    } else {
        let log = usize::BITS - (n - 1).leading_zeros();
        mul(mul(n, log as usize)?, 64)?
    };
    u64::try_from(work).map_err(|_| Error::Overflow)
}

struct Admission {
    limits: Limits,
    bytes: usize,
    peak: usize,
    work: u64,
    fixed: usize,
}
impl Admission {
    fn new(limits: Limits) -> Self {
        Self {
            limits,
            bytes: 0,
            peak: 0,
            work: 0,
            fixed: 0,
        }
    }
    fn ensure_bytes(&self, bytes: usize) -> Result<(), Error> {
        if add(add(self.bytes, self.fixed)?, bytes)? > self.limits.transient_bytes {
            return Err(Error::Refused("transient capacity ceiling"));
        }
        Ok(())
    }
    fn account(&mut self, bytes: usize) -> Result<(), Error> {
        self.ensure_bytes(bytes)?;
        self.bytes = add(self.bytes, bytes)?;
        self.peak = self.peak.max(self.bytes);
        Ok(())
    }
    fn fixed(&mut self, bytes: usize) -> Result<(), Error> {
        self.ensure_bytes(bytes)?;
        self.fixed = add(self.fixed, bytes)?;
        Ok(())
    }
    fn release(&mut self, bytes: usize) {
        self.bytes -= bytes;
    }
    fn charge(&mut self, operations: u64) -> Result<(), Error> {
        let next = sum(self.work, operations)?;
        if next > self.limits.operations {
            return Err(Error::Refused("compute ceiling"));
        }
        self.work = next;
        Ok(())
    }
    fn vector<T>(&mut self, count: usize) -> Result<Vec<T>, Error> {
        self.ensure_bytes(mul(count, size_of::<T>())?)?;
        let mut v = Vec::new();
        v.try_reserve_exact(count)
            .map_err(|_| Error::Refused("allocation"))?;
        self.account(mul(v.capacity(), size_of::<T>())?)?;
        Ok(v)
    }
    fn memory(&self, retained: usize) -> Memory {
        Memory {
            peak_capacity_bytes: self.peak,
            retained_capacity_bytes: retained,
            fixed_array_bytes: self.fixed,
        }
    }
}
fn capacity<T>(v: &Vec<T>) -> Result<usize, Error> {
    mul(v.capacity(), size_of::<T>())
}

/// Immutable borrowed parameters in the exact native-spec order.
#[derive(Debug)]
pub struct Model<'a> {
    dimension: usize,
    side: usize,
    parameters: &'a [f32],
    sha256: Hash,
}
impl<'a> Model<'a> {
    pub fn new(dimension: usize, side: usize, parameters: &'a [f32]) -> Result<Self, Error> {
        if parameters.len() != parameter_count(dimension, side)?
            || parameters.iter().any(|p| !p.is_finite())
        {
            return Err(Error::Invalid("model parameters"));
        }
        let mut digest = Sha256::new();
        digest.update(b"BORSUK-budget-model-v1");
        digest.update((dimension as u32).to_le_bytes());
        digest.update((side as u32).to_le_bytes());
        for p in parameters {
            digest.update(p.to_bits().to_le_bytes());
        }
        Ok(Self {
            dimension,
            side,
            parameters,
            sha256: digest.finalize().into(),
        })
    }
    pub fn dimension(&self) -> usize {
        self.dimension
    }
    pub fn side(&self) -> usize {
        self.side
    }
    pub fn sha256(&self) -> Hash {
        self.sha256
    }
    pub fn borrowed_parameter_bytes(&self) -> usize {
        self.parameters.len() * size_of::<f32>()
    }
    pub fn owned_capacity_bytes(&self) -> usize {
        0
    }
    pub fn validation_operations(&self) -> u64 {
        // One finite check and four canonical bytes per parameter.
        (self.parameters.len() * 5 + 30) as u64
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Object {
    pub groups: u16,
    pub rows: u16,
    pub body_bytes: u64,
}

/// Group ordinal is its position in owners. Thus every original group occurs
/// exactly once, and only the final ordinal can be a short group.
#[derive(Debug)]
pub struct Membership<'a> {
    dimension: usize,
    side: usize,
    total_rows: u64,
    owners: &'a [u32],
    digests: &'a [Hash],
    objects: Vec<Object>,
    sha256: Hash,
    storage: Storage,
}
impl<'a> Membership<'a> {
    pub fn new(
        dimension: usize,
        side: usize,
        total_rows: u64,
        owners: &'a [u32],
        digests: &'a [Hash],
        limits: Limits,
    ) -> Result<Self, Error> {
        let labels = geometry(dimension, side)?;
        let groups = total_rows.checked_add(15).ok_or(Error::Overflow)? / 16;
        if groups != owners.len() as u64
            || digests.len() != labels
            || groups > mul(labels, 64)? as u64
            || total_rows > mul(labels, 1024)? as u64
        {
            return Err(Error::Invalid("membership geometry"));
        }
        let mut admission = Admission::new(limits);
        let canonical_bytes = add(add(mul(owners.len(), 4)?, mul(labels, 32)?)?, 56)?;
        admission.charge(sum(
            sum(sum(owners.len() as u64, labels as u64)?, sort_work(labels)?)?,
            canonical_bytes as u64,
        )?)?;
        let mut objects = admission.vector::<Object>(labels)?;
        objects.resize(labels, Object::default());
        for (g, &owner) in owners.iter().enumerate() {
            let object = objects
                .get_mut(owner as usize)
                .ok_or(Error::Invalid("group owner"))?;
            let rows = if g + 1 == owners.len() && total_rows % 16 != 0 {
                (total_rows % 16) as u16
            } else {
                16
            };
            object.groups = object.groups.checked_add(1).ok_or(Error::Overflow)?;
            object.rows = object.rows.checked_add(rows).ok_or(Error::Overflow)?;
            if object.groups > 64 || object.rows > 1024 {
                return Err(Error::Invalid("object capacity"));
            }
        }
        let mut nonempty = admission.vector::<Hash>(labels)?;
        for (object, digest) in objects.iter_mut().zip(digests) {
            if (object.rows == 0) != (*digest == [0; 32]) {
                return Err(Error::Invalid("digest occupancy"));
            }
            if object.rows != 0 {
                object.body_bytes = sum(
                    64,
                    u64::from(object.rows)
                        .checked_mul((dimension + 12) as u64)
                        .ok_or(Error::Overflow)?,
                )?;
                nonempty.push(*digest);
            }
        }
        nonempty.sort_unstable();
        if nonempty.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::Invalid("duplicate digest"));
        }
        let mut digest = Sha256::new();
        digest.update(b"BORSUK-budget-membership-v1");
        digest.update((dimension as u32).to_le_bytes());
        digest.update((side as u32).to_le_bytes());
        digest.update(total_rows.to_le_bytes());
        digest.update((owners.len() as u64).to_le_bytes());
        for owner in owners {
            digest.update(owner.to_le_bytes());
        }
        for body_digest in digests {
            digest.update(body_digest);
        }
        let owned_capacity_bytes = capacity(&objects)?;
        let storage = Storage {
            borrowed_bytes: add(mul(owners.len(), 4)?, mul(digests.len(), 32)?)?,
            offline_group_owner_bytes: mul(owners.len(), 4)?,
            borrowed_directory_bytes: mul(digests.len(), 32)?,
            owned_capacity_bytes,
            validation_peak_capacity_bytes: admission.peak,
            validation_operations: admission.work,
        };
        Ok(Self {
            dimension,
            side,
            total_rows,
            owners,
            digests,
            objects,
            sha256: digest.finalize().into(),
            storage,
        })
    }
    pub fn sha256(&self) -> Hash {
        self.sha256
    }
    pub fn storage(&self) -> Storage {
        self.storage
    }
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }
    pub fn owners(&self) -> &[u32] {
        self.owners
    }
    pub fn digests(&self) -> &[Hash] {
        self.digests
    }
    pub fn objects(&self) -> &[Object] {
        &self.objects
    }
    fn group_rows(&self, g: usize) -> u16 {
        if g + 1 == self.owners.len() && self.total_rows % 16 != 0 {
            (self.total_rows % 16) as u16
        } else {
            16
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Charge {
    pub reads: u64,
    pub bytes: u64,
}
impl Charge {
    pub const ZERO: Self = Self { reads: 0, bytes: 0 };
    fn plus(self, other: Self) -> Result<Self, Error> {
        Ok(Self {
            reads: sum(self.reads, other.reads)?,
            bytes: sum(self.bytes, other.bytes)?,
        })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    pub max_reads: u64,
    pub max_bytes: u64,
    pub head: Charge,
    pub root: Charge,
    pub delta: Charge,
    pub attempts: Charge,
}
impl Budget {
    fn reservations(self) -> Result<Charge, Error> {
        if self.max_reads > MAX_READS || self.max_bytes > MAX_BYTES {
            return Err(Error::Invalid("read/byte cap"));
        }
        self.head
            .plus(self.root)?
            .plus(self.delta)?
            .plus(self.attempts)
    }
    fn fits(self, charge: Charge) -> bool {
        charge.reads <= self.max_reads && charge.bytes <= self.max_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub root: Hash,
    pub coefficients: Hash,
    pub delta: Hash,
    pub revision: u64,
}
impl Snapshot {
    fn validate(self) -> Result<(), Error> {
        if self.root == [0; 32] || self.coefficients == [0; 32] || self.delta == [0; 32] {
            return Err(Error::Invalid("snapshot identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupWeight {
    pub group: u32,
    pub weight: u16,
}

fn neighbor_hash(weights: &[GroupWeight]) -> Hash {
    let mut h = Sha256::new();
    h.update(b"BORSUK-budget-neighbors-v1");
    h.update((weights.len() as u64).to_le_bytes());
    for w in weights {
        h.update(w.group.to_le_bytes());
        h.update(w.weight.to_le_bytes());
    }
    h.finalize().into()
}
fn identity_work(d: usize, neighbors: usize) -> Result<u64, Error> {
    // Query validation/norm and canonical SHA byte processing. Snapshot
    // identities are fixed-size values copied into the binding.
    Ok(add(add(mul(d, 8)?, mul(neighbors, 6)?)?, 64)? as u64)
}
fn validate_weights(
    membership: &Membership<'_>,
    weights: &[GroupWeight],
    admission: &mut Admission,
) -> Result<(), Error> {
    if weights.len() > membership.owners.len() {
        return Err(Error::Invalid("neighbor count"));
    }
    admission.charge(sum(weights.len() as u64, sort_work(weights.len())?)?)?;
    let mut ordinals = admission.vector::<u32>(weights.len())?;
    for w in weights {
        if w.group as usize >= membership.owners.len()
            || w.weight == 0
            || w.weight > membership.group_rows(w.group as usize)
        {
            return Err(Error::Invalid("neighbor weight"));
        }
        ordinals.push(w.group);
    }
    ordinals.sort_unstable();
    if ordinals.windows(2).any(|w| w[0] == w[1]) {
        return Err(Error::Invalid("duplicate neighbor group"));
    }
    admission.release(capacity(&ordinals)?);
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Identity {
    pub model: Hash,
    pub membership: Hash,
    pub query: Hash,
    pub snapshot: Snapshot,
}
fn identity(
    model: &Model<'_>,
    membership: &Membership<'_>,
    query: &[f32],
    snapshot: Snapshot,
) -> Result<Identity, Error> {
    snapshot.validate()?;
    if model.dimension != membership.dimension || model.side != membership.side {
        return Err(Error::Invalid("model/membership geometry"));
    }
    if query.len() != model.dimension || query.iter().any(|x| !x.is_finite()) {
        return Err(Error::Invalid("query"));
    }
    let norm: f64 = query.iter().map(|&x| f64::from(x).powi(2)).sum();
    if !norm.is_finite() || norm <= 0. {
        return Err(Error::Invalid("query norm"));
    }
    let mut h = Sha256::new();
    h.update(b"BORSUK-budget-query-v1");
    h.update((query.len() as u32).to_le_bytes());
    for x in query {
        h.update(x.to_bits().to_le_bytes());
    }
    Ok(Identity {
        model: model.sha256,
        membership: membership.sha256,
        query: h.finalize().into(),
        snapshot,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Binding {
    pub identity: Identity,
    pub neighbors: Hash,
    pub budget: Budget,
    pub requested_rows: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Score {
    pub label: u32,
    pub probability: f32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelectedObject {
    pub label: u32,
    pub rows: u16,
    pub body_bytes: u64,
    pub reserved_bytes: u64,
    pub digest: Hash,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SkippedObject {
    pub label: u32,
    pub required: Charge,
    pub remaining: Charge,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    Reservations,
    NoBodyFits,
    ZeroGain,
    LowerTailRegression,
    IncompleteSelection,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    Selected,
    Underfill { requested: u64, visible: u64 },
    Refused(Refusal),
}

/// A bound diagnostic plan, not authority to dispatch a real object request.
/// Fields are private to prevent caller alteration after identity checking.
#[derive(Debug, PartialEq)]
pub struct Plan {
    binding: Binding,
    candidates: Vec<u32>,
    occupied_candidates: Vec<Score>,
    selected: Vec<SelectedObject>,
    skipped: Vec<SkippedObject>,
    charge: Charge,
    actual_modeled_charge: Charge,
    visible_rows: u64,
    covered_weight: u64,
    disposition: Disposition,
    work: Work,
    memory: Memory,
}
impl Plan {
    pub fn binding(&self) -> Binding {
        self.binding
    }
    pub fn candidates(&self) -> &[u32] {
        &self.candidates
    }
    pub fn occupied_candidates(&self) -> &[Score] {
        &self.occupied_candidates
    }
    pub fn selected(&self) -> &[SelectedObject] {
        &self.selected
    }
    pub fn skipped(&self) -> &[SkippedObject] {
        &self.skipped
    }
    /// Metadata declarations plus complete maximum-body serving reservations.
    pub fn reserved_charge(&self) -> Charge {
        self.charge
    }
    /// Exact membership-derived body sizes, diagnostic only. These smaller
    /// sizes never finance an additional serving selection or real transfer.
    pub fn actual_modeled_charge(&self) -> Charge {
        self.actual_modeled_charge
    }
    pub fn visible_rows(&self) -> u64 {
        self.visible_rows
    }
    pub fn covered_weight(&self) -> u64 {
        self.covered_weight
    }
    pub fn disposition(&self) -> Disposition {
        self.disposition
    }
    pub fn work(&self) -> Work {
        self.work
    }
    pub fn memory(&self) -> Memory {
        self.memory
    }
    #[allow(clippy::too_many_arguments)]
    pub fn validate(
        &self,
        model: &Model<'_>,
        membership: &Membership<'_>,
        query: &[f32],
        neighbors: &[GroupWeight],
        snapshot: Snapshot,
        budget: Budget,
        requested_rows: u64,
    ) -> Result<(), Error> {
        if neighbors.len() > membership.owners.len() {
            return Err(Error::Invalid("neighbor count"));
        }
        let bound = Binding {
            identity: identity(model, membership, query, snapshot)?,
            neighbors: neighbor_hash(neighbors),
            budget,
            requested_rows,
        };
        if self.binding != bound {
            return Err(Error::IdentityMismatch);
        }
        Ok(())
    }
}

fn finite(value: f32) -> Result<f32, Error> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(Error::Invalid("nonfinite intermediate"))
    }
}
fn linear(weights: &[f32], input: &[f32], bias: f32) -> Result<f32, Error> {
    let mut dot = 0f32;
    for (&w, &x) in weights.iter().zip(input) {
        dot = finite(dot + finite(w * x)?)?;
    }
    finite(dot + bias)
}
fn softmax(values: &mut [f32]) -> Result<(), Error> {
    let maximum = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    finite(maximum)?;
    let mut divisor = 0f32;
    for x in values.iter_mut() {
        *x = finite(finite(*x - maximum)?.exp())?;
        divisor = finite(divisor + *x)?;
    }
    if divisor <= 0. {
        return Err(Error::Invalid("softmax divisor"));
    }
    for x in values {
        *x = finite(*x / divisor)?;
    }
    Ok(())
}

// Inference keeps only 4 + 8*s probabilities; parameters remain borrowed.
fn probabilities(
    model: &Model<'_>,
    query: &[f32],
    admission: &mut Admission,
) -> Result<Vec<f32>, Error> {
    let d = model.dimension;
    let s = model.side;
    let count = add(MIXTURES, mul(8, s)?)?;
    // Two units per dot multiply/add, four per softmax entry, and input/norm
    // validation. Canonical hash bytes are separately admitted by each caller.
    let dots = add(mul(HIDDEN, d)?, mul(add(MIXTURES, mul(8, s)?)?, HIDDEN)?)?;
    admission.charge(add(add(mul(dots, 2)?, mul(count, 4)?)?, mul(d, 4)?)? as u64)?;
    admission.ensure_bytes(mul(d, size_of::<f32>())?)?;
    let normalized = crate::sq8_source::cosine_vector(query)
        .map_err(|_| Error::Invalid("query normalization"))?;
    if let Cow::Owned(v) = &normalized {
        admission.account(capacity(v)?)?;
    }
    let mut values = admission.vector::<f32>(count)?;
    let mut hidden = [0f32; HIDDEN];
    let p = model.parameters;
    let hidden_bias = HIDDEN * d;
    for (h, value) in hidden.iter_mut().enumerate() {
        *value = linear(&p[h * d..(h + 1) * d], &normalized, p[hidden_bias + h])?.max(0.);
    }
    let mixture_weights = hidden_bias + HIDDEN;
    let mixture_bias = mixture_weights + MIXTURES * HIDDEN;
    for r in 0..MIXTURES {
        values.push(linear(
            &p[mixture_weights + r * HIDDEN..mixture_weights + (r + 1) * HIDDEN],
            &hidden,
            p[mixture_bias + r],
        )?);
    }
    softmax(&mut values[..MIXTURES])?;
    let mut offset = mixture_bias + MIXTURES;
    for _ in 0..2 {
        let head_bias = offset + MIXTURES * s * HIDDEN;
        for r in 0..MIXTURES {
            let start = values.len();
            for i in 0..s {
                let w = offset + (r * s + i) * HIDDEN;
                values.push(linear(
                    &p[w..w + HIDDEN],
                    &hidden,
                    p[head_bias + r * s + i],
                )?);
            }
            softmax(&mut values[start..])?;
        }
        offset = head_bias + MIXTURES * s;
    }
    if let Cow::Owned(v) = &normalized {
        admission.release(capacity(v)?);
    }
    Ok(values)
}
fn top_head(values: &[f32]) -> [usize; HEAD_TOP] {
    let mut top = [usize::MAX; HEAD_TOP];
    for (i, &score) in values.iter().enumerate() {
        for slot in 0..HEAD_TOP {
            let old = top[slot];
            if old == usize::MAX || score > values[old] || (score == values[old] && i < old) {
                top.copy_within(slot..HEAD_TOP - 1, slot + 1);
                top[slot] = i;
                break;
            }
        }
    }
    top
}
fn model_score(p: &[f32], s: usize, label: u32) -> Result<f32, Error> {
    let i = label as usize / s;
    let j = label as usize % s;
    let mut score = 0f32;
    for r in 0..MIXTURES {
        let first = finite(p[r] * p[MIXTURES + r * s + i])?;
        let term = finite(first * p[MIXTURES + MIXTURES * s + r * s + j])?;
        score = finite(score + term)?;
    }
    Ok(score)
}
fn coverage(
    membership: &Membership<'_>,
    weights: &[GroupWeight],
    labels: impl Fn(u32) -> bool,
) -> u64 {
    weights
        .iter()
        .filter(|w| labels(membership.owners[w.group as usize]))
        .map(|w| u64::from(w.weight))
        .sum()
}

#[allow(clippy::too_many_arguments)]
pub fn select(
    model: &Model<'_>,
    membership: &Membership<'_>,
    query: &[f32],
    neighbors: &[GroupWeight],
    snapshot: Snapshot,
    budget: Budget,
    requested_rows: u64,
    limits: Limits,
) -> Result<Plan, Error> {
    let reserved = budget.reservations()?;
    let mut admission = Admission::new(limits);
    admission.fixed(HIDDEN * size_of::<f32>() + 2 * HEAD_TOP * size_of::<usize>())?;
    validate_weights(membership, neighbors, &mut admission)?;
    admission.charge(identity_work(model.dimension, neighbors.len())?)?;
    let binding = Binding {
        identity: identity(model, membership, query, snapshot)?,
        neighbors: neighbor_hash(neighbors),
        budget,
        requested_rows,
    };
    let p = probabilities(model, query, &mut admission)?;
    admission.charge(sum(
        sum(
            (8 * model.side * HEAD_TOP) as u64,
            sort_work(MAX_CANDIDATES)?,
        )?,
        sum(
            sort_work(MAX_CANDIDATES)?,
            add(
                MAX_CANDIDATES * MIXTURES * 3 + MAX_CANDIDATES,
                mul(neighbors.len(), MAX_SELECTED)?,
            )? as u64,
        )?,
    )?)?;
    let mut candidates = admission.vector::<u32>(MAX_CANDIDATES)?;
    let s = model.side;
    for r in 0..MIXTURES {
        let first = top_head(&p[MIXTURES + r * s..MIXTURES + (r + 1) * s]);
        let second =
            top_head(&p[MIXTURES + MIXTURES * s + r * s..MIXTURES + MIXTURES * s + (r + 1) * s]);
        for &i in &first[..s.min(HEAD_TOP)] {
            for &j in &second[..s.min(HEAD_TOP)] {
                candidates.push((i * s + j) as u32);
            }
        }
    }
    candidates.sort_unstable();
    candidates.dedup();
    let mut occupied_candidates = admission.vector::<Score>(MAX_CANDIDATES)?;
    for &label in &candidates {
        if membership.objects[label as usize].rows != 0 {
            occupied_candidates.push(Score {
                label,
                probability: model_score(&p, s, label)?,
            });
        }
    }
    occupied_candidates.sort_unstable_by(|a, b| {
        b.probability
            .total_cmp(&a.probability)
            .then(a.label.cmp(&b.label))
    });
    let mut selected = admission.vector::<SelectedObject>(MAX_SELECTED)?;
    let mut skipped = admission.vector::<SkippedObject>(MAX_CANDIDATES)?;
    let mut charge = reserved;
    let mut actual_modeled_charge = reserved;
    let body_reservation = max_body_reservation_bytes(model.dimension)?;
    let mut visible_rows = 0;
    let reservation_refused =
        reserved.reads >= budget.max_reads || reserved.bytes >= budget.max_bytes;
    if !reservation_refused {
        for score in &occupied_candidates {
            if selected.len() == MAX_SELECTED {
                break;
            }
            let object = membership.objects[score.label as usize];
            let required = Charge {
                reads: 1,
                bytes: body_reservation,
            };
            let next = charge.plus(required)?;
            if budget.fits(next) {
                charge = next;
                actual_modeled_charge = actual_modeled_charge.plus(Charge {
                    reads: 1,
                    bytes: object.body_bytes,
                })?;
                visible_rows = sum(visible_rows, u64::from(object.rows))?;
                selected.push(SelectedObject {
                    label: score.label,
                    rows: object.rows,
                    body_bytes: object.body_bytes,
                    reserved_bytes: body_reservation,
                    digest: membership.digests[score.label as usize],
                });
            } else {
                skipped.push(SkippedObject {
                    label: score.label,
                    required,
                    remaining: Charge {
                        reads: budget.max_reads - charge.reads,
                        bytes: budget.max_bytes - charge.bytes,
                    },
                });
            }
        }
    }
    let covered_weight = coverage(membership, neighbors, |label| {
        selected.iter().any(|s| s.label == label)
    });
    let disposition = if reservation_refused {
        Disposition::Refused(Refusal::Reservations)
    } else if selected.is_empty() && !occupied_candidates.is_empty() {
        Disposition::Refused(Refusal::NoBodyFits)
    } else if visible_rows < requested_rows {
        Disposition::Underfill {
            requested: requested_rows,
            visible: visible_rows,
        }
    } else {
        Disposition::Selected
    };
    let retained = add(
        add(capacity(&candidates)?, capacity(&occupied_candidates)?)?,
        add(capacity(&selected)?, capacity(&skipped)?)?,
    )?;
    Ok(Plan {
        binding,
        candidates,
        occupied_candidates,
        selected,
        skipped,
        charge,
        actual_modeled_charge,
        visible_rows,
        covered_weight,
        disposition,
        work: Work {
            operations: admission.work,
        },
        memory: admission.memory(retained),
    })
}

/// Separately charged full-grid ranking over occupied labels only. Empty
/// labels are excluded before ranking; this is never called by select.
#[derive(Debug, PartialEq)]
pub struct ModelRanking {
    pub identity: Identity,
    pub ranked: Vec<Score>,
    pub work: Work,
    pub memory: Memory,
}
impl ModelRanking {
    pub fn winner_omitted(&self, plan: &Plan) -> Result<bool, Error> {
        if self.identity != plan.binding.identity {
            return Err(Error::IdentityMismatch);
        }
        Ok(self
            .ranked
            .first()
            .is_some_and(|s| !plan.candidates.contains(&s.label)))
    }
}
pub fn full_grid_rank(
    model: &Model<'_>,
    membership: &Membership<'_>,
    query: &[f32],
    snapshot: Snapshot,
    limits: Limits,
) -> Result<ModelRanking, Error> {
    let mut admission = Admission::new(limits);
    admission.fixed(HIDDEN * size_of::<f32>())?;
    admission.charge(identity_work(model.dimension, 0)?)?;
    let identity = identity(model, membership, query, snapshot)?;
    let labels = geometry(model.dimension, model.side)?;
    admission.charge(sum(
        sort_work(labels)?,
        mul(labels, MIXTURES * 3 + 1)? as u64,
    )?)?;
    let p = probabilities(model, query, &mut admission)?;
    let mut ranked = admission.vector::<Score>(labels)?;
    for label in 0..labels {
        if membership.objects[label].rows == 0 {
            continue;
        }
        ranked.push(Score {
            label: label as u32,
            probability: model_score(&p, model.side, label as u32)?,
        });
    }
    ranked.sort_unstable_by(|a, b| {
        b.probability
            .total_cmp(&a.probability)
            .then(a.label.cmp(&b.label))
    });
    let memory = admission.memory(capacity(&ranked)?);
    Ok(ModelRanking {
        identity,
        ranked,
        work: Work {
            operations: admission.work,
        },
        memory,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub struct Cover {
    pub labels: Vec<u32>,
    pub covered_weight: u64,
    pub charge: Charge,
}
#[derive(Debug, PartialEq, Eq)]
pub struct CoverOracle {
    pub membership: Hash,
    pub neighbors: Hash,
    pub budget: Budget,
    pub refusal: Option<Refusal>,
    /// Exact-size ideal is unavailable to the digest-only serving format.
    pub exact_size_unavailable_to_serving_format: bool,
    pub legal: Cover,
    pub candidate_restricted: Cover,
    pub work: Work,
    pub memory: Memory,
}
fn prefer(hits: u64, charge: Charge, labels: &[u32], best: &Cover) -> bool {
    hits > best.covered_weight
        || (hits == best.covered_weight
            && (charge.bytes, charge.reads, labels)
                < (best.charge.bytes, best.charge.reads, best.labels.as_slice()))
}
/// Exact-size offline ideal only, with strict tiny-input ceilings. This is
/// unavailable to the digest-only serving format and cannot finance reads.
/// Candidate-restricted cover uses these same ideal constraints separately.
pub fn best_cover(
    membership: &Membership<'_>,
    neighbors: &[GroupWeight],
    candidates: &[u32],
    budget: Budget,
    max_selected: usize,
    limits: Limits,
) -> Result<CoverOracle, Error> {
    if membership.owners.len() > 12 || max_selected > 4 {
        return Err(Error::Refused("exact cover ceiling"));
    }
    if candidates.len() > MAX_CANDIDATES
        || candidates
            .iter()
            .any(|&l| l as usize >= membership.objects.len())
    {
        return Err(Error::Invalid("candidate geometry"));
    }
    let reserved = budget.reservations()?;
    let mut admission = Admission::new(limits);
    admission.fixed((12 + 4) * size_of::<u32>())?;
    let mut occupied = [0u32; 12];
    let mut n = 0;
    admission.charge(membership.objects.len() as u64)?;
    for (label, object) in membership.objects.iter().enumerate() {
        if object.rows != 0 {
            if n == occupied.len() {
                return Err(Error::Refused("occupied object oracle ceiling"));
            }
            occupied[n] = label as u32;
            n += 1;
        }
    }
    validate_weights(membership, neighbors, &mut admission)?;
    let subsets = 1usize.checked_shl(n as u32).ok_or(Error::Overflow)?;
    admission.charge(identity_work(0, neighbors.len())?)?;
    admission.charge(mul(subsets, add(mul(12, MAX_CANDIDATES)?, 12 * 12)?)? as u64)?;
    let mut legal = Cover {
        labels: admission.vector::<u32>(4)?,
        covered_weight: 0,
        charge: reserved,
    };
    let mut candidate_restricted = Cover {
        labels: admission.vector::<u32>(4)?,
        covered_weight: 0,
        charge: reserved,
    };
    if budget.fits(reserved) {
        for mask in 0..subsets {
            if mask.count_ones() as usize > max_selected {
                continue;
            }
            let mut labels = [0u32; 4];
            let mut count = 0;
            let mut charge = reserved;
            for (bit, &label) in occupied[..n].iter().enumerate() {
                if mask & (1 << bit) != 0 {
                    labels[count] = label;
                    count += 1;
                    charge = charge.plus(Charge {
                        reads: 1,
                        bytes: membership.objects[label as usize].body_bytes,
                    })?;
                }
            }
            if !budget.fits(charge) {
                continue;
            }
            let labels = &labels[..count];
            let hits = coverage(membership, neighbors, |label| labels.contains(&label));
            if prefer(hits, charge, labels, &legal) {
                legal.labels.clear();
                legal.labels.extend_from_slice(labels);
                legal.covered_weight = hits;
                legal.charge = charge;
            }
            if labels.iter().all(|l| candidates.contains(l))
                && prefer(hits, charge, labels, &candidate_restricted)
            {
                candidate_restricted.labels.clear();
                candidate_restricted.labels.extend_from_slice(labels);
                candidate_restricted.covered_weight = hits;
                candidate_restricted.charge = charge;
            }
        }
    }
    let memory = admission.memory(add(
        capacity(&legal.labels)?,
        capacity(&candidate_restricted.labels)?,
    )?);
    Ok(CoverOracle {
        membership: membership.sha256,
        neighbors: neighbor_hash(neighbors),
        budget,
        refusal: if reserved.reads >= budget.max_reads || reserved.bytes >= budget.max_bytes {
            Some(Refusal::Reservations)
        } else {
            None
        },
        exact_size_unavailable_to_serving_format: true,
        legal,
        candidate_restricted,
        work: Work {
            operations: admission.work,
        },
        memory,
    })
}

#[derive(Clone, Copy)]
pub struct Evaluation<'a> {
    pub model: &'a Model<'a>,
    pub membership: &'a Membership<'a>,
    pub snapshot: Snapshot,
}
#[derive(Clone, Copy)]
pub struct TrainingSample<'a> {
    pub query: &'a [f32],
    pub neighbors: &'a [GroupWeight],
    pub requested_rows: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptanceDecision {
    Accepted,
    Refused(Refusal),
}
#[derive(Debug, PartialEq, Eq)]
pub struct Acceptance {
    pub before_model: Hash,
    pub after_model: Hash,
    pub before_membership: Hash,
    pub after_membership: Hash,
    pub before_snapshot: Snapshot,
    pub after_snapshot: Snapshot,
    pub training: Hash,
    pub budget: Budget,
    pub before_total: u64,
    pub after_total: u64,
    pub before_lower_tail: u64,
    pub after_lower_tail: u64,
    pub lower_tail_rank: usize,
    pub decision: AcceptanceDecision,
    pub modeled_reads: u64,
    pub reserved_bytes: u64,
    pub actual_modeled_bytes: u64,
    pub work: Work,
    pub memory: Memory,
}
impl Acceptance {
    pub fn accepted(&self) -> bool {
        self.decision == AcceptanceDecision::Accepted
    }
}

fn same_population(before: Evaluation<'_>, after: Evaluation<'_>) -> Result<(), Error> {
    let a = before.membership;
    let b = after.membership;
    if a.dimension != b.dimension
        || a.side != b.side
        || a.total_rows != b.total_rows
        || a.owners.len() != b.owners.len()
    {
        return Err(Error::Invalid("changed group population or frozen grid"));
    }
    if before.snapshot.coefficients != after.snapshot.coefficients {
        return Err(Error::Invalid("changed coefficients"));
    }
    Ok(())
}
/// Caller provides both immutable memberships and their body digests. This
/// validates a one-group move or two-group swap and never mutates either input.
pub fn evaluate_move(
    before: Evaluation<'_>,
    after: Evaluation<'_>,
    samples: &[TrainingSample<'_>],
    budget: Budget,
    limits: Limits,
) -> Result<Acceptance, Error> {
    same_population(before, after)?;
    if before.model.sha256 != after.model.sha256 {
        return Err(Error::Invalid("move changed model"));
    }
    let scan = before.membership.owners.len() as u64;
    let remaining = limits
        .operations
        .checked_sub(scan)
        .ok_or(Error::Refused("move validation compute ceiling"))?;
    let transient_bytes = limits
        .transient_bytes
        .checked_sub(4 * size_of::<u32>())
        .ok_or(Error::Refused("move validation fixed arrays"))?;
    let mut changes = [(0u32, 0u32); 2];
    let mut n = 0;
    for (&a, &b) in before.membership.owners.iter().zip(after.membership.owners) {
        if a != b {
            if n == 2 {
                return Err(Error::Invalid("not a move/swap"));
            }
            changes[n] = (a, b);
            n += 1;
        }
    }
    if n == 2 && changes[0] != (changes[1].1, changes[1].0) {
        return Err(Error::Invalid("not a single-group swap"));
    }
    let mut receipt = evaluate_checkpoint(
        before,
        after,
        samples,
        budget,
        Limits {
            operations: remaining,
            transient_bytes,
        },
    )?;
    receipt.work.operations = sum(receipt.work.operations, scan)?;
    receipt.memory.fixed_array_bytes = add(receipt.memory.fixed_array_bytes, 4 * size_of::<u32>())?;
    if add(
        receipt.memory.peak_capacity_bytes,
        receipt.memory.fixed_array_bytes,
    )? > limits.transient_bytes
    {
        return Err(Error::Refused("move coexistence ceiling"));
    }
    Ok(receipt)
}
/// Pure checkpoint decision using actual serving selection. p05 uses the
/// nearest-rank statistic, giving exactly the 13th-smallest of 256 anchors.
/// This evaluates declared training observations; it does not fit a model.
pub fn evaluate_checkpoint(
    before: Evaluation<'_>,
    after: Evaluation<'_>,
    samples: &[TrainingSample<'_>],
    budget: Budget,
    limits: Limits,
) -> Result<Acceptance, Error> {
    same_population(before, after)?;
    if samples.is_empty() || samples.len() > 256 {
        return Err(Error::Invalid("training count"));
    }
    let fixed_arrays = 2 * 256 * size_of::<u64>();
    let transient_bytes = limits
        .transient_bytes
        .checked_sub(fixed_arrays)
        .ok_or(Error::Refused("training fixed arrays"))?;
    let mut before_hits = [0u64; 256];
    let mut after_hits = [0u64; 256];
    let mut before_total = 0;
    let mut after_total = 0;
    let mut work = sum(
        sum(sort_work(samples.len())?, sort_work(samples.len())?)?,
        add(mul(samples.len(), 72)?, 40)? as u64,
    )?;
    if work > limits.operations {
        return Err(Error::Refused("checkpoint compute ceiling"));
    }
    let mut peak = 0usize;
    let mut modeled_reads = 0;
    let mut modeled_bytes = 0;
    let mut actual_modeled_bytes = 0;
    let mut incomplete = false;
    let mut training = Sha256::new();
    training.update(b"BORSUK-budget-training-v1");
    training.update((samples.len() as u32).to_le_bytes());
    for (i, sample) in samples.iter().enumerate() {
        let remaining = Limits {
            transient_bytes,
            operations: limits.operations - work,
        };
        let a = select(
            before.model,
            before.membership,
            sample.query,
            sample.neighbors,
            before.snapshot,
            budget,
            sample.requested_rows,
            remaining,
        )?;
        work = sum(work, a.work.operations)?;
        // The first plan's output remains live during the second evaluation.
        let after_limits = Limits {
            transient_bytes: transient_bytes
                .checked_sub(a.memory.retained_capacity_bytes)
                .ok_or(Error::Refused("checkpoint coexistence"))?,
            operations: limits.operations - work,
        };
        let b = select(
            after.model,
            after.membership,
            sample.query,
            sample.neighbors,
            after.snapshot,
            budget,
            sample.requested_rows,
            after_limits,
        )?;
        work = sum(work, b.work.operations)?;
        peak = peak.max(a.memory.peak_capacity_bytes).max(add(
            a.memory.retained_capacity_bytes,
            b.memory.peak_capacity_bytes,
        )?);
        training.update(a.binding.identity.query);
        training.update(a.binding.neighbors);
        training.update(sample.requested_rows.to_le_bytes());
        before_hits[i] = a.covered_weight;
        after_hits[i] = b.covered_weight;
        before_total = sum(before_total, a.covered_weight)?;
        after_total = sum(after_total, b.covered_weight)?;
        modeled_reads = sum(modeled_reads, sum(a.charge.reads, b.charge.reads)?)?;
        modeled_bytes = sum(modeled_bytes, sum(a.charge.bytes, b.charge.bytes)?)?;
        actual_modeled_bytes = sum(
            actual_modeled_bytes,
            sum(a.actual_modeled_charge.bytes, b.actual_modeled_charge.bytes)?,
        )?;
        incomplete |=
            a.disposition != Disposition::Selected || b.disposition != Disposition::Selected;
    }
    before_hits[..samples.len()].sort_unstable();
    after_hits[..samples.len()].sort_unstable();
    let lower_tail_rank = (samples.len() * 5).div_ceil(100);
    let before_lower_tail = before_hits[lower_tail_rank - 1];
    let after_lower_tail = after_hits[lower_tail_rank - 1];
    let decision = if incomplete {
        AcceptanceDecision::Refused(Refusal::IncompleteSelection)
    } else if after_total <= before_total {
        AcceptanceDecision::Refused(Refusal::ZeroGain)
    } else if after_lower_tail < before_lower_tail {
        AcceptanceDecision::Refused(Refusal::LowerTailRegression)
    } else {
        AcceptanceDecision::Accepted
    };
    Ok(Acceptance {
        before_model: before.model.sha256,
        after_model: after.model.sha256,
        before_membership: before.membership.sha256,
        after_membership: after.membership.sha256,
        before_snapshot: before.snapshot,
        after_snapshot: after.snapshot,
        training: training.finalize().into(),
        budget,
        before_total,
        after_total,
        before_lower_tail,
        after_lower_tail,
        lower_tail_rank,
        decision,
        modeled_reads,
        reserved_bytes: modeled_bytes,
        actual_modeled_bytes,
        work: Work { operations: work },
        memory: Memory {
            peak_capacity_bytes: peak,
            retained_capacity_bytes: 0,
            fixed_array_bytes: fixed_arrays
                + HIDDEN * size_of::<f32>()
                + 2 * HEAD_TOP * size_of::<usize>(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits {
            transient_bytes: 16 * 1024 * 1024,
            operations: 250_000_000,
        }
    }

    fn snapshot() -> Snapshot {
        Snapshot {
            root: [1; 32],
            coefficients: [2; 32],
            delta: [3; 32],
            revision: 7,
        }
    }

    // Independent fixture encoder: output logits are log probabilities; all
    // input-dependent weights are zero. This does not use production offsets.
    fn parameters(d: usize, s: usize, heads: Option<&[Vec<f32>]>) -> Vec<f32> {
        let mut p = vec![0.; 64 * d];
        p.extend([0.; 64]);
        p.extend([0.; 4 * 64]);
        p.extend([0.; 4]);
        for _ in 0..2 {
            p.extend(vec![0.; 4 * s * 64]);
            for r in 0..4 {
                for i in 0..s {
                    p.push(heads.map_or(0., |h| h[r][i].ln()));
                }
            }
        }
        p
    }

    fn digests(s: usize, owners: &[u32]) -> Vec<[u8; 32]> {
        let mut result = vec![[0; 32]; s * s];
        for &owner in owners {
            let mut digest = [7; 32];
            digest[..4].copy_from_slice(&(owner + 1).to_le_bytes());
            result[owner as usize] = digest;
        }
        result
    }

    fn membership<'a>(
        s: usize,
        rows: u64,
        owners: &'a [u32],
        hashes: &'a [[u8; 32]],
    ) -> Membership<'a> {
        Membership::new(1, s, rows, owners, hashes, limits()).unwrap()
    }

    fn budget(reads: u64, bytes: u64) -> Budget {
        Budget {
            max_reads: reads,
            max_bytes: bytes,
            head: Charge::ZERO,
            root: Charge::ZERO,
            delta: Charge::ZERO,
            attempts: Charge::ZERO,
        }
    }

    // Independent serving expectation for the fixed 2x2 fixture: model order
    // is known analytically as [0,1,2,3]. Rows and coverage are computed from
    // original group ordinals, never production membership/coverage helpers.
    fn expected(owners: &[u32], weights: &[u16], reads: u64, bytes: u64) -> (Vec<u32>, u64) {
        let mut selected = Vec::new();
        let mut remaining = bytes;
        for label in 0..4 {
            let rows: u64 = owners
                .iter()
                .enumerate()
                .filter(|(_, o)| **o == label)
                .map(|(g, _)| if g == 5 { 5 } else { 16 })
                .sum();
            let reservation = 64 + 1024 * 13;
            if rows > 0 && selected.len() < reads as usize && reservation <= remaining {
                remaining -= reservation;
                selected.push(label);
            }
        }
        let hits = owners
            .iter()
            .zip(weights)
            .filter(|(o, _)| selected.contains(o))
            .map(|(_, w)| u64::from(*w))
            .sum();
        (selected, hits)
    }

    fn independent_cover(
        owners: &[u32],
        weights: &[u16],
        reads: u64,
        bytes: u64,
        allowed: &[u32],
    ) -> u64 {
        let mut best = 0;
        for mask in 0u32..16 {
            if u64::from(mask.count_ones()) > reads {
                continue;
            }
            let labels: Vec<u32> = (0..4).filter(|l| mask & (1 << l) != 0).collect();
            if labels
                .iter()
                .any(|l| !allowed.contains(l) || !owners.contains(l))
            {
                continue;
            }
            let mut cost = 64 * labels.len() as u64;
            let mut hits = 0;
            for (g, &o) in owners.iter().enumerate() {
                if labels.contains(&o) {
                    cost += 13 * if g == 5 { 5 } else { 16 };
                    hits += u64::from(weights[g]);
                }
            }
            if cost <= bytes {
                best = best.max(hits);
            }
        }
        best
    }

    #[test]
    fn oracle_exhaustive_six_group_four_label_capacity_two() {
        let heads = vec![vec![0.8, 0.2]; 4];
        let p = parameters(1, 2, Some(&heads));
        let model = Model::new(1, 2, &p).unwrap();
        let weights = [1, 3, 4, 6, 8, 5];
        let neighbors: Vec<GroupWeight> = weights
            .iter()
            .enumerate()
            .map(|(g, &weight)| GroupWeight {
                group: g as u32,
                weight,
            })
            .collect();
        let b = budget(2, 2 * (64 + 1024 * 13));
        let ideal_budget = budget(2, 800);
        let mut assignments = 0;
        for code in 0..4096u32 {
            let owners: Vec<u32> = (0..6).map(|g| (code >> (2 * g)) & 3).collect();
            if (0..4).any(|l| owners.iter().filter(|&&o| o == l).count() > 2) {
                continue;
            }
            assignments += 1;
            let hashes = digests(2, &owners);
            let m = membership(2, 85, &owners, &hashes);
            let plan = select(&model, &m, &[1.], &neighbors, snapshot(), b, 1, limits()).unwrap();
            let (labels, hits) = expected(&owners, &weights, 2, 2 * (64 + 1024 * 13));
            assert_eq!(
                plan.selected.iter().map(|o| o.label).collect::<Vec<_>>(),
                labels
            );
            assert_eq!(plan.covered_weight, hits);
            let cover =
                best_cover(&m, &neighbors, &plan.candidates, ideal_budget, 2, limits()).unwrap();
            assert!(cover.exact_size_unavailable_to_serving_format);
            assert_eq!(
                cover.legal.covered_weight,
                independent_cover(&owners, &weights, 2, 800, &[0, 1, 2, 3])
            );
            assert_eq!(
                cover.candidate_restricted.covered_weight,
                independent_cover(&owners, &weights, 2, 800, &plan.candidates)
            );
            let samples = [TrainingSample {
                query: &[1.],
                neighbors: &neighbors,
                requested_rows: 1,
            }];
            for g in 0..6 {
                for destination in 0..4 {
                    if destination == owners[g]
                        || owners.iter().filter(|&&o| o == destination).count() == 2
                    {
                        continue;
                    }
                    let mut after = owners.clone();
                    after[g] = destination;
                    let after_hashes = digests(2, &after);
                    let after_m = membership(2, 85, &after, &after_hashes);
                    let receipt = evaluate_move(
                        Evaluation {
                            model: &model,
                            membership: &m,
                            snapshot: snapshot(),
                        },
                        Evaluation {
                            model: &model,
                            membership: &after_m,
                            snapshot: snapshot(),
                        },
                        &samples,
                        b,
                        limits(),
                    )
                    .unwrap();
                    let (_, after_hits) = expected(&after, &weights, 2, 2 * (64 + 1024 * 13));
                    assert_eq!(
                        (receipt.before_total, receipt.after_total),
                        (hits, after_hits)
                    );
                    assert_eq!(receipt.accepted(), after_hits > hits);
                }
            }
        }
        assert_eq!(assignments, 1440);
    }

    #[test]
    fn oracle_mixture_33_omitted_global_winner_and_ties() {
        let epsilon = 1e-7f32;
        let heads: Vec<Vec<f32>> = (0..4)
            .map(|r| {
                (0..33)
                    .map(|i| {
                        if i == 32 {
                            9. / 89.
                        } else if i / 8 == r {
                            10. / 89.
                        } else {
                            epsilon
                        }
                    })
                    .collect()
            })
            .collect();
        let p = parameters(1, 33, Some(&heads));
        let model = Model::new(1, 33, &p).unwrap();
        let owners: Vec<u32> = (0..1089).collect();
        let hashes = digests(33, &owners);
        let m = membership(33, 1089 * 16, &owners, &hashes);
        let plan = select(
            &model,
            &m,
            &[1.],
            &[],
            snapshot(),
            budget(32, 16 << 20),
            100,
            limits(),
        )
        .unwrap();
        assert_eq!(plan.candidates.len(), 256);
        assert!(!plan.candidates.contains(&1088));
        let ranking = full_grid_rank(&model, &m, &[1.], snapshot(), limits()).unwrap();
        assert_eq!(ranking.ranked[0].label, 1088);
        assert!(ranking.winner_omitted(&plan).unwrap());
        // Independent normalized probability sum (f64), without score helpers.
        let mut expected_scores = Vec::new();
        for i in 0..33 {
            for j in 0..33 {
                let score: f64 = heads
                    .iter()
                    .map(|h| {
                        let z: f64 = h.iter().map(|&x| f64::from(x)).sum();
                        0.25 * f64::from(h[i]) * f64::from(h[j]) / (z * z)
                    })
                    .sum();
                expected_scores.push(((i * 33 + j) as u32, score));
            }
        }
        expected_scores.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        assert_eq!(expected_scores[0].0, ranking.ranked[0].label);
        for scored in &ranking.ranked {
            let wanted = expected_scores
                .iter()
                .find(|x| x.0 == scored.label)
                .unwrap()
                .1;
            assert!((f64::from(scored.probability) - wanted).abs() < 2e-8);
        }
        // Duplicate components, uniform ties, and empty labels: only the two
        // occupied shortlist labels are selected, in increasing label order.
        let tied_p = parameters(1, 9, None);
        let tied_model = Model::new(1, 9, &tied_p).unwrap();
        let tied_owners = [0, 1, 80];
        let tied_hashes = digests(9, &tied_owners);
        let tied_m = membership(9, 48, &tied_owners, &tied_hashes);
        let tied = select(
            &tied_model,
            &tied_m,
            &[1.],
            &[],
            snapshot(),
            budget(32, 16 << 20),
            100,
            limits(),
        )
        .unwrap();
        let wanted: Vec<u32> = (0..8)
            .flat_map(|i| (0..8).map(move |j| i * 9 + j))
            .collect();
        assert_eq!(tied.candidates, wanted);
        assert_eq!(
            tied.selected.iter().map(|o| o.label).collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(tied.charge.reads, 2);
        assert_eq!(tied.visible_rows, 32);
        assert!(matches!(tied.disposition, Disposition::Underfill { .. }));
        let common_neighbors = [GroupWeight {
            group: 2,
            weight: 16,
        }];
        let cover = best_cover(
            &tied_m,
            &common_neighbors,
            &tied.candidates,
            budget(32, 16 << 20),
            2,
            limits(),
        )
        .unwrap();
        // Independently: only label 80 contains positive weight; it costs
        // 272 bytes and is absent from the emitted Cartesian set.
        assert_eq!(cover.legal.covered_weight, 16);
        assert_eq!(cover.legal.labels, [80]);
        assert_eq!(cover.candidate_restricted.covered_weight, 0);
        let sparse_heads = vec![vec![0.2, 0.8]; 4];
        let sparse_p = parameters(1, 2, Some(&sparse_heads));
        let sparse_model = Model::new(1, 2, &sparse_p).unwrap();
        let sparse_owners = [0, 1];
        let sparse_hashes = digests(2, &sparse_owners);
        let sparse = membership(2, 32, &sparse_owners, &sparse_hashes);
        let occupied_rank =
            full_grid_rank(&sparse_model, &sparse, &[1.], snapshot(), limits()).unwrap();
        // Analytic scores: empty label 3 has 0.64, empty label 2 has
        // 0.16. Of occupied labels, 1 has 0.16 and 0 has 0.04.
        assert_eq!(
            occupied_rank
                .ranked
                .iter()
                .map(|s| s.label)
                .collect::<Vec<_>>(),
            [1, 0]
        );
        let sparse_plan = select(
            &sparse_model,
            &sparse,
            &[1.],
            &[],
            snapshot(),
            budget(32, 16 << 20),
            1,
            limits(),
        )
        .unwrap();
        assert!(!occupied_rank.winner_omitted(&sparse_plan).unwrap());
    }

    #[test]
    fn oracle_scale_s335_bounded_candidates_and_charges() {
        let p = parameters(768, 335, None);
        assert_eq!(p.len(), 64 * 769 + 4 * 65 + 8 * 335 * 65);
        let model = Model::new(768, 335, &p).unwrap();
        let owners: Vec<u32> = (0..112225).collect();
        let hashes = digests(335, &owners);
        let m = Membership::new(
            768,
            335,
            owners.len() as u64 * 16,
            &owners,
            &hashes,
            limits(),
        )
        .unwrap();
        let query = vec![1.; 768];
        let plan = select(
            &model,
            &m,
            &query,
            &[],
            snapshot(),
            budget(32, 16 << 20),
            100,
            limits(),
        )
        .unwrap();
        let wanted: Vec<u32> = (0..8)
            .flat_map(|i| (0..8).map(move |j| i * 335 + j))
            .collect();
        assert_eq!(plan.candidates, wanted);
        assert!(plan.candidates.len() <= 256);
        assert_eq!(plan.selected.len(), 15);
        assert_eq!(
            plan.charge,
            Charge {
                reads: 15,
                bytes: 15 * (64 + 1024 * (768 + 12))
            }
        );
        assert_eq!(
            plan.actual_modeled_charge,
            Charge {
                reads: 15,
                bytes: 15 * (64 + 16 * (768 + 12))
            }
        );
        assert!(plan.work.operations < limits().operations);
        assert!(plan.memory.peak_capacity_bytes <= limits().transient_bytes);
        assert_eq!(model.borrowed_parameter_bytes(), p.len() * 4);
        assert_eq!(model.owned_capacity_bytes(), 0);
        assert!(m.storage().owned_capacity_bytes >= 112225 * std::mem::size_of::<Object>());
    }

    #[test]
    fn oracle_finite_mlp_nonunit_query_matches_independent_scalar() {
        // Independent typed tensors and encoder; no production parameter
        // offsets, normalization, inference, score, or candidate helpers.
        let mut hidden_weights = [[0f32; 3]; 64];
        let mut hidden_biases = [0f32; 64];
        let mut mixture_weights = [[0f32; 64]; 4];
        let mut mixture_biases = [0f32; 4];
        let mut first_weights = [[[0f32; 64]; 9]; 4];
        let mut first_biases = [[0f32; 9]; 4];
        let mut second_weights = [[[0f32; 64]; 9]; 4];
        let mut second_biases = [[0f32; 9]; 4];
        for h in 0..64 {
            for k in 0..3 {
                hidden_weights[h][k] = (((h * 3 + k) % 17) as f32 - 7.75) / 64.;
            }
            hidden_biases[h] = if h % 2 == 0 { 0.125 } else { -0.125 };
        }
        for r in 0..4 {
            mixture_biases[r] = (r as f32 - 1.5) / 16.;
            for h in 0..64 {
                mixture_weights[r][h] = (((r * 7 + h * 3) % 19) as f32 - 8.75) / 512.;
            }
            for i in 0..9 {
                first_biases[r][i] = (((r * 3 + i * 2) % 11) as f32 - 4.75) / 32.;
                second_biases[r][i] = (((r * 5 + i * 3) % 13) as f32 - 5.75) / 16.;
                for h in 0..64 {
                    first_weights[r][i][h] = (((r * 11 + i * 5 + h) % 23) as f32 - 10.75) / 1024.;
                    second_weights[r][i][h] =
                        (((r * 13 + i * 7 + h * 2) % 29) as f32 - 13.75) / 2048.;
                }
            }
        }
        let mut encoded = Vec::new();
        encoded.extend(hidden_weights.iter().flatten().copied());
        encoded.extend(hidden_biases);
        encoded.extend(mixture_weights.iter().flatten().copied());
        encoded.extend(mixture_biases);
        for tensor in first_weights {
            for row in tensor {
                encoded.extend(row);
            }
        }
        for row in first_biases {
            encoded.extend(row);
        }
        for tensor in second_weights {
            for row in tensor {
                encoded.extend(row);
            }
        }
        for row in second_biases {
            encoded.extend(row);
        }
        let model = Model::new(3, 9, &encoded).unwrap();
        let owners: Vec<u32> = (0..81).collect();
        let hashes = digests(9, &owners);
        let m = Membership::new(3, 9, 81 * 16, &owners, &hashes, limits()).unwrap();
        let query = [3f32, -4., 12.];
        let normalized: [f32; 3] = std::array::from_fn(|i| (f64::from(query[i]) / 13.) as f32);
        fn scalar_dot(weights: &[f32], input: &[f32], bias: f32) -> f32 {
            let mut result = 0f32;
            for i in 0..input.len() {
                let product = weights[i] * input[i];
                result += product;
            }
            result + bias
        }
        fn scalar_softmax(logits: &[f32]) -> Vec<f32> {
            let mut maximum = logits[0];
            for &value in &logits[1..] {
                if value > maximum {
                    maximum = value;
                }
            }
            let mut result = Vec::new();
            let mut denominator = 0f32;
            for &value in logits {
                let exponent = (value - maximum).exp();
                denominator += exponent;
                result.push(exponent);
            }
            for value in &mut result {
                *value /= denominator;
            }
            result
        }
        let mut hidden = [0f32; 64];
        let mut positive = 0;
        let mut negative = 0;
        for h in 0..64 {
            let preactivation = scalar_dot(&hidden_weights[h], &normalized, hidden_biases[h]);
            positive += usize::from(preactivation > 0.);
            negative += usize::from(preactivation < 0.);
            hidden[h] = if preactivation < 0. {
                0.
            } else {
                preactivation
            };
        }
        assert!(positive > 0 && negative > 0);
        let mixture_logits: Vec<f32> = (0..4)
            .map(|r| scalar_dot(&mixture_weights[r], &hidden, mixture_biases[r]))
            .collect();
        let mixture = scalar_softmax(&mixture_logits);
        assert!(mixture.windows(2).any(|w| w[0] != w[1]));
        let mut first = Vec::new();
        let mut second = Vec::new();
        let mut shortlist = Vec::new();
        let mut different_heads = false;
        for r in 0..4 {
            let u: Vec<f32> = (0..9)
                .map(|i| scalar_dot(&first_weights[r][i], &hidden, first_biases[r][i]))
                .collect();
            let v: Vec<f32> = (0..9)
                .map(|j| scalar_dot(&second_weights[r][j], &hidden, second_biases[r][j]))
                .collect();
            let u = scalar_softmax(&u);
            let v = scalar_softmax(&v);
            let mut u_order: Vec<usize> = (0..9).collect();
            let mut v_order: Vec<usize> = (0..9).collect();
            u_order.sort_by(|&a, &b| u[b].total_cmp(&u[a]).then(a.cmp(&b)));
            v_order.sort_by(|&a, &b| v[b].total_cmp(&v[a]).then(a.cmp(&b)));
            different_heads |= u_order != v_order;
            for &i in u_order.iter().take(8) {
                for &j in v_order.iter().take(8) {
                    shortlist.push((i * 9 + j) as u32);
                }
            }
            first.push(u);
            second.push(v);
        }
        assert!(different_heads);
        shortlist.sort_unstable();
        shortlist.dedup();
        let mut scores = Vec::new();
        for i in 0..9 {
            for j in 0..9 {
                let mut score = 0f32;
                for r in 0..4 {
                    let weighted_first = mixture[r] * first[r][i];
                    let component = weighted_first * second[r][j];
                    score += component;
                }
                scores.push(((i * 9 + j) as u32, score));
            }
        }
        scores.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        let ranking = full_grid_rank(&model, &m, &query, snapshot(), limits()).unwrap();
        assert_eq!(ranking.ranked.len(), 81);
        for (actual, &(label, score)) in ranking.ranked.iter().zip(&scores) {
            assert_eq!(
                (actual.label, actual.probability.to_bits()),
                (label, score.to_bits())
            );
        }
        let actual = select(
            &model,
            &m,
            &query,
            &[],
            snapshot(),
            budget(32, 16 << 20),
            100,
            limits(),
        )
        .unwrap();
        assert_eq!(actual.candidates, shortlist);
        let wanted: Vec<(u32, u32)> = scores
            .iter()
            .filter(|(l, _)| shortlist.contains(l))
            .map(|&(l, p)| (l, p.to_bits()))
            .collect();
        assert_eq!(
            actual
                .occupied_candidates
                .iter()
                .map(|s| (s.label, s.probability.to_bits()))
                .collect::<Vec<_>>(),
            wanted
        );
        assert_eq!(
            actual.selected.iter().map(|s| s.label).collect::<Vec<_>>(),
            wanted.iter().take(15).map(|s| s.0).collect::<Vec<_>>()
        );
        assert_eq!(actual.charge.bytes, 15 * (64 + 1024 * 15));
        assert_eq!(actual.actual_modeled_charge.bytes, 15 * (64 + 16 * 15));
    }

    #[test]
    fn oracle_budget_boundaries_overflow_and_finite_validation() {
        let r = 64 + 1024 * 13;
        let p = parameters(1, 2, None);
        let model = Model::new(1, 2, &p).unwrap();
        let owners = [0, 1, 2];
        let hashes = digests(2, &owners);
        let m = membership(2, 33, &owners, &hashes);
        let exact = select(
            &model,
            &m,
            &[2.],
            &[],
            snapshot(),
            budget(2, r),
            16,
            limits(),
        )
        .unwrap();
        assert_eq!(exact.selected.len(), 1);
        assert_eq!(exact.charge.bytes, r);
        assert_eq!(exact.actual_modeled_charge.bytes, 272);
        let short = select(
            &model,
            &m,
            &[2.],
            &[],
            snapshot(),
            budget(2, r - 1),
            16,
            limits(),
        )
        .unwrap();
        assert!(short.selected.is_empty());
        assert_eq!(short.visible_rows, 0);
        assert_eq!(short.skipped.len(), 3);
        assert_eq!(short.disposition, Disposition::Refused(Refusal::NoBodyFits));
        // The one-row tail still reserves r, rather than financing a read
        // with its 77-byte diagnostic body size.
        assert_eq!(short.skipped[2].required.bytes, r);
        let mut b = budget(3, 1000);
        b.head = Charge {
            reads: 1,
            bytes: 100,
        };
        b.root = Charge {
            reads: 1,
            bytes: 100,
        };
        b.delta = Charge {
            reads: 1,
            bytes: 100,
        };
        let refused = select(&model, &m, &[1.], &[], snapshot(), b, 1, limits()).unwrap();
        assert_eq!(
            refused.charge,
            Charge {
                reads: 3,
                bytes: 300
            }
        );
        assert_eq!(
            refused.disposition,
            Disposition::Refused(Refusal::Reservations)
        );
        b.delta.bytes = 801;
        let over = select(&model, &m, &[1.], &[], snapshot(), b, 1, limits()).unwrap();
        assert_eq!(over.charge.bytes, 1001);
        assert_eq!(
            over.disposition,
            Disposition::Refused(Refusal::Reservations)
        );
        let mut partial = budget(32, 200 + 3 * r);
        partial.head = Charge {
            reads: 1,
            bytes: 100,
        };
        partial.root = Charge {
            reads: 1,
            bytes: 100,
        };
        let full = select(&model, &m, &[1.], &[], snapshot(), partial, 33, limits()).unwrap();
        assert_eq!(full.selected.len(), 3);
        assert_eq!(
            full.charge,
            Charge {
                reads: 5,
                bytes: 200 + 3 * r
            }
        );
        assert_eq!(full.actual_modeled_charge.bytes, 821);
        partial.delta = Charge {
            reads: 1,
            bytes: 100,
        };
        let reduced = select(&model, &m, &[1.], &[], snapshot(), partial, 33, limits()).unwrap();
        assert_eq!(
            reduced.selected.iter().map(|s| s.label).collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(
            reduced.charge,
            Charge {
                reads: 5,
                bytes: 300 + 2 * r
            }
        );
        assert_eq!(reduced.actual_modeled_charge.bytes, 844);
        assert_eq!(
            reduced.disposition,
            Disposition::Underfill {
                requested: 33,
                visible: 32
            }
        );
        partial.attempts = Charge {
            reads: 27,
            bytes: 0,
        };
        let two = select(&model, &m, &[1.], &[], snapshot(), partial, 1, limits()).unwrap();
        assert_eq!(two.selected.len(), 2);
        assert_eq!(two.charge.reads, 32);
        partial.attempts.reads += 1;
        let one = select(&model, &m, &[1.], &[], snapshot(), partial, 1, limits()).unwrap();
        assert_eq!(one.selected.len(), 1);
        assert_eq!(one.charge.reads, 32);
        b.delta.bytes = u64::MAX;
        assert_eq!(
            select(&model, &m, &[1.], &[], snapshot(), b, 1, limits()).unwrap_err(),
            Error::Overflow
        );
        for query in [vec![0.], vec![f32::NAN], vec![f32::INFINITY], vec![]] {
            assert!(
                select(
                    &model,
                    &m,
                    &query,
                    &[],
                    snapshot(),
                    budget(32, 1000),
                    1,
                    limits()
                )
                .is_err()
            );
        }
        let mut bad = p.clone();
        bad[0] = f32::NAN;
        assert!(Model::new(1, 2, &bad).is_err());
        bad[0] = f32::MAX;
        bad[64] = f32::MAX;
        let finite_model = Model::new(1, 2, &bad).unwrap();
        assert!(matches!(
            select(
                &finite_model,
                &m,
                &[1.],
                &[],
                snapshot(),
                budget(32, 1000),
                1,
                limits()
            ),
            Err(Error::Invalid(_))
        ));
        assert!(Model::new(0, 2, &p).is_err());
        assert!(Model::new(1, 336, &p).is_err());
        assert!(Membership::new(1, 2, u64::MAX, &owners, &hashes, limits()).is_err());
        let outside = [4];
        assert!(Membership::new(1, 2, 16, &outside, &hashes, limits()).is_err());
        let mut bad_hashes = hashes.clone();
        bad_hashes[3] = [9; 32];
        assert!(Membership::new(1, 2, 33, &owners, &bad_hashes, limits()).is_err());
        bad_hashes[3] = [0; 32];
        bad_hashes[1] = bad_hashes[0];
        assert!(Membership::new(1, 2, 33, &owners, &bad_hashes, limits()).is_err());
        let too_many = vec![0; 65];
        let too_many_hashes = digests(2, &too_many);
        assert!(Membership::new(1, 2, 65 * 16, &too_many, &too_many_hashes, limits()).is_err());
        let low_memory = Limits {
            transient_bytes: 0,
            operations: 100_000_000,
        };
        assert!(matches!(
            select(
                &model,
                &m,
                &[1.],
                &[],
                snapshot(),
                budget(32, 1000),
                1,
                low_memory
            ),
            Err(Error::Refused(_))
        ));
        let low_compute = Limits {
            transient_bytes: 1 << 20,
            operations: 1,
        };
        assert!(matches!(
            select(
                &model,
                &m,
                &[1.],
                &[],
                snapshot(),
                budget(32, 1000),
                1,
                low_compute
            ),
            Err(Error::Refused(_))
        ));
        assert!(matches!(
            best_cover(&m, &[], &[0], budget(32, 1000), 5, limits()),
            Err(Error::Refused(_))
        ));
        let duplicate = [GroupWeight {
            group: 0,
            weight: 1,
        }; 2];
        assert!(
            select(
                &model,
                &m,
                &[1.],
                &duplicate,
                snapshot(),
                budget(32, 1000),
                1,
                limits()
            )
            .is_err()
        );
        let oversized_weight = [GroupWeight {
            group: 2,
            weight: 2,
        }];
        assert!(
            select(
                &model,
                &m,
                &[1.],
                &oversized_weight,
                snapshot(),
                budget(32, 1000),
                1,
                limits()
            )
            .is_err()
        );
    }

    #[test]
    fn oracle_identity_binding_and_determinism() {
        let empty_p = parameters(1, 2, None);
        let empty_model = Model::new(1, 2, &empty_p).unwrap();
        let empty_hashes = [[0; 32]; 4];
        let empty = membership(2, 0, &[], &empty_hashes);
        let empty_plan = select(
            &empty_model,
            &empty,
            &[1.],
            &[],
            snapshot(),
            budget(32, 1000),
            1,
            limits(),
        )
        .unwrap();
        assert_eq!(empty_plan.charge, Charge::ZERO);
        assert_eq!(empty_plan.occupied_candidates.len(), 0);
        let empty_ranking =
            full_grid_rank(&empty_model, &empty, &[1.], snapshot(), limits()).unwrap();
        assert!(empty_ranking.ranked.is_empty());
        assert!(!empty_ranking.winner_omitted(&empty_plan).unwrap());
        assert_eq!(
            empty_plan.disposition,
            Disposition::Underfill {
                requested: 1,
                visible: 0
            }
        );
        let p = parameters(1, 2, None);
        let model = Model::new(1, 2, &p).unwrap();
        let owners = [0, 1];
        let hashes = digests(2, &owners);
        let m = membership(2, 32, &owners, &hashes);
        let neighbors = [GroupWeight {
            group: 0,
            weight: 1,
        }];
        let b = budget(32, 1000);
        let first = select(&model, &m, &[1.], &neighbors, snapshot(), b, 1, limits()).unwrap();
        let repeat = select(&model, &m, &[1.], &neighbors, snapshot(), b, 1, limits()).unwrap();
        assert_eq!(first, repeat);
        first
            .validate(&model, &m, &[1.], &neighbors, snapshot(), b, 1)
            .unwrap();
        assert_eq!(
            first.validate(&model, &m, &[2.], &neighbors, snapshot(), b, 1),
            Err(Error::IdentityMismatch)
        );
        for field in 0..4 {
            let mut changed = snapshot();
            match field {
                0 => changed.root[0] ^= 1,
                1 => changed.coefficients[0] ^= 1,
                2 => changed.delta[0] ^= 1,
                _ => changed.revision += 1,
            }
            assert_eq!(
                first.validate(&model, &m, &[1.], &neighbors, changed, b, 1),
                Err(Error::IdentityMismatch)
            );
        }
        let mut changed_p = p.clone();
        changed_p[0] = 1.;
        let changed_model = Model::new(1, 2, &changed_p).unwrap();
        assert_eq!(
            first.validate(&changed_model, &m, &[1.], &neighbors, snapshot(), b, 1),
            Err(Error::IdentityMismatch)
        );
        let changed_owners = [1, 0];
        let changed_m = membership(2, 32, &changed_owners, &hashes);
        assert_eq!(
            first.validate(&model, &changed_m, &[1.], &neighbors, snapshot(), b, 1),
            Err(Error::IdentityMismatch)
        );
        assert_eq!(
            first.validate(
                &model,
                &m,
                &[1.],
                &neighbors,
                snapshot(),
                budget(31, 1000),
                1
            ),
            Err(Error::IdentityMismatch)
        );
        assert_eq!(
            first.validate(&model, &m, &[1.], &neighbors, snapshot(), b, 2),
            Err(Error::IdentityMismatch)
        );
        let changed_neighbors = [GroupWeight {
            group: 0,
            weight: 2,
        }];
        assert_eq!(
            first.validate(&model, &m, &[1.], &changed_neighbors, snapshot(), b, 1),
            Err(Error::IdentityMismatch)
        );
    }

    #[test]
    fn oracle_move_swap_and_checkpoint_strict_acceptance() {
        let p = parameters(1, 2, None);
        let model = Model::new(1, 2, &p).unwrap();
        let before_owners = [0, 0, 1, 1, 2, 3];
        let after_owners = [0, 1, 0, 1, 2, 3]; // Capacity-preserving single-group swap.
        let before_hashes = digests(2, &before_owners);
        let after_hashes = digests(2, &after_owners);
        let before = membership(2, 85, &before_owners, &before_hashes);
        let after = membership(2, 85, &after_owners, &after_hashes);
        let weights = [1, 1, 8, 1, 1, 1];
        let neighbors: Vec<GroupWeight> = weights
            .iter()
            .enumerate()
            .map(|(g, &weight)| GroupWeight {
                group: g as u32,
                weight,
            })
            .collect();
        let sample = TrainingSample {
            query: &[1.],
            neighbors: &neighbors,
            requested_rows: 1,
        };
        let samples = [sample; 256];
        let b = budget(1, 64 + 1024 * 13);
        let eval_before = Evaluation {
            model: &model,
            membership: &before,
            snapshot: snapshot(),
        };
        let eval_after = Evaluation {
            model: &model,
            membership: &after,
            snapshot: snapshot(),
        };
        let moved = evaluate_move(eval_before, eval_after, &samples, b, limits()).unwrap();
        let before_hits = expected(&before_owners, &weights, 1, 64 + 1024 * 13).1;
        let after_hits = expected(&after_owners, &weights, 1, 64 + 1024 * 13).1;
        assert_eq!(
            (moved.before_total, moved.after_total),
            (before_hits * 256, after_hits * 256)
        );
        assert_eq!(
            (
                moved.before_lower_tail,
                moved.after_lower_tail,
                moved.lower_tail_rank
            ),
            (before_hits, after_hits, 13)
        );
        assert!(moved.accepted());
        let zero = evaluate_move(eval_before, eval_before, &samples, b, limits()).unwrap();
        assert_eq!(
            zero.decision,
            AcceptanceDecision::Refused(Refusal::ZeroGain)
        );
        let checkpoint =
            evaluate_checkpoint(eval_before, eval_after, &samples, b, limits()).unwrap();
        assert!(checkpoint.accepted());
        let many_changed = [1, 1, 0, 0, 3, 2];
        let many_hashes = digests(2, &many_changed);
        let many = membership(2, 85, &many_changed, &many_hashes);
        assert!(
            evaluate_move(
                eval_before,
                Evaluation {
                    membership: &many,
                    ..eval_after
                },
                &samples,
                b,
                limits()
            )
            .is_err()
        );
        // Independent group counting: every initial hit is two; twenty
        // anchors lose one while the others gain eight. Total improves but
        // the 13th-smallest hit falls from two to one.
        let low_before = [
            GroupWeight {
                group: 0,
                weight: 1,
            },
            GroupWeight {
                group: 1,
                weight: 1,
            },
        ];
        let high_before = [
            GroupWeight {
                group: 0,
                weight: 2,
            },
            GroupWeight {
                group: 2,
                weight: 8,
            },
        ];
        let regression: Vec<TrainingSample> = (0..256)
            .map(|i| TrainingSample {
                query: &[1.],
                neighbors: if i < 20 { &low_before } else { &high_before },
                requested_rows: 1,
            })
            .collect();
        let rejected =
            evaluate_checkpoint(eval_before, eval_after, &regression, b, limits()).unwrap();
        assert_eq!(
            (rejected.before_total, rejected.after_total),
            (512, 20 + 236 * 10)
        );
        assert_eq!(
            (rejected.before_lower_tail, rejected.after_lower_tail),
            (2, 1)
        );
        assert_eq!(
            rejected.decision,
            AcceptanceDecision::Refused(Refusal::LowerTailRegression)
        );
        let changed_heads = vec![vec![0.2, 0.8]; 4];
        let changed_p = parameters(1, 2, Some(&changed_heads));
        let changed_model = Model::new(1, 2, &changed_p).unwrap();
        let changed = Evaluation {
            model: &changed_model,
            ..eval_before
        };
        let tail = [GroupWeight {
            group: 5,
            weight: 5,
        }];
        let tail_samples = [TrainingSample {
            query: &[1.],
            neighbors: &tail,
            requested_rows: 1,
        }; 256];
        // Analytic model ranks: uniform ties choose label 0; changed heads
        // choose label 3, containing the five-row tail group.
        let fitted = evaluate_checkpoint(eval_before, changed, &tail_samples, b, limits()).unwrap();
        assert_eq!((fitted.before_total, fitted.after_total), (0, 256 * 5));
        assert_eq!((fitted.before_lower_tail, fitted.after_lower_tail), (0, 5));
        assert!(fitted.accepted());
        assert!(evaluate_move(eval_before, changed, &tail_samples, b, limits()).is_err());
        let underfilled_samples = [TrainingSample {
            requested_rows: 100,
            ..tail_samples[0]
        }];
        let underfilled =
            evaluate_checkpoint(eval_before, changed, &underfilled_samples, b, limits()).unwrap();
        assert_eq!(
            underfilled.decision,
            AcceptanceDecision::Refused(Refusal::IncompleteSelection)
        );
    }
}
