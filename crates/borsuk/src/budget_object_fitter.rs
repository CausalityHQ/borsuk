//! Bounded, pure fitting of declared training data. No authenticated teacher,
//! held data, I/O, publication, or recall qualification is provided here.

use crate::budget_object_selector::{
    self as selector, Acceptance, Admission, Budget, Error, HIDDEN, Hash, Limits, MIXTURES,
    Membership, Memory, Model, PreparedCoverage, PreparedQuery, PreparedTraining, Snapshot,
    TrainingSample, Work, add, capacity, finite, linear, mul, parameter_count, softmax, sum,
};
use sha2::{Digest, Sha256};
use std::mem::size_of;

pub const SEED: u64 = 20260923;
pub const MAX_OPERATIONS: u64 = 512_000_000_000;
pub const MAX_CAPACITY_BYTES: usize = 8 * 1024 * 1024 * 1024;
const EPOCHS: usize = 16;
const BATCH: usize = 16;
const ROUNDS: usize = 2;
const RATE: f64 = 1. / 256.;
const EPSILON: f64 = 1. / 16_777_216.;

/// Only declared training observations are accepted. Caller constructs the
/// source anchor order and teacher; this module does not authenticate either.
#[derive(Clone, Copy)]
pub struct FitInput<'a> {
    pub source: Hash,
    pub dimension: usize,
    pub rows: u64,
    pub anchors: &'a [TrainingSample<'a>],
    pub snapshot: Snapshot,
    pub budget: Budget,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress {
    pub round: usize,
    pub epoch: usize,
    pub batch: usize,
    pub group: Option<u32>,
    pub operations: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FitEvent {
    Checkpoint {
        round: usize,
        receipt: Acceptance,
    },
    Proposal {
        round: usize,
        group: u32,
        destination: u32,
        swap: Option<u32>,
        receipt: Acceptance,
    },
    NoTeacher {
        round: usize,
        group: u32,
    },
    CapacityRefused {
        round: usize,
        group: u32,
        destination: u32,
    },
}

/// Returned only after both complete alternating rounds. Any resource/caller
/// refusal returns Err and drops every partial candidate, never a trained result.
#[derive(Debug)]
pub struct FitResult {
    pub source: Hash,
    pub training: Hash,
    pub snapshot: Snapshot,
    pub budget: Budget,
    pub parameters: Vec<f32>,
    pub owners: Vec<u32>,
    /// Membership-derived offline tokens, never authenticated payload hashes.
    pub diagnostic_tokens: Vec<Hash>,
    pub side: usize,
    pub model: Hash,
    pub membership: Hash,
    pub events: Vec<FitEvent>,
    pub work: Work,
    pub memory: Memory,
    /// Live borrowed anchor/slice bytes, admitted alongside owned capacities.
    pub borrowed_input_bytes: usize,
}

/// An incomplete/refused fit has no model or membership output. Charges
/// describe work already performed and peak admitted capacity before refusal.
#[derive(Debug, PartialEq, Eq)]
pub struct FitFailure {
    pub error: Error,
    pub work: Work,
    pub memory: Memory,
    pub completed: bool,
    pub stage: FitStage,
    /// Initial declared state or the last strictly accepted checkpoint/proposal;
    /// identities only, with no partial parameters or membership output.
    pub accepted: Option<FitIdentity>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FitStage {
    Admission,
    Initialization,
    ObservationPreparation,
    InitialCoverage,
    Training {
        round: usize,
    },
    Checkpoint {
        round: usize,
    },
    Destinations {
        round: usize,
        group: u32,
    },
    Proposal {
        round: usize,
        group: u32,
        destination: u32,
    },
    Finalization,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FitIdentity {
    pub source: Hash,
    pub training: Hash,
    pub model: Hash,
    pub membership: Hash,
    pub snapshot: Snapshot,
    pub budget: Budget,
}
struct FitEvidence {
    stage: FitStage,
    accepted: Option<FitIdentity>,
}

fn remaining(a: &Admission) -> Result<Limits, Error> {
    Ok(Limits {
        transient_bytes: a
            .limits
            .transient_bytes
            .checked_sub(add(a.bytes, a.fixed)?)
            .ok_or(Error::Refused("fitting coexistence"))?,
        operations: a
            .limits
            .operations
            .checked_sub(a.work)
            .ok_or(Error::Refused("fitting work"))?,
    })
}
fn absorb(a: &mut Admission, work: u64, memory: Memory) -> Result<(), Error> {
    a.charge(work)?;
    let peak = add(memory.peak_capacity_bytes, memory.fixed_array_bytes)?;
    a.ensure_bytes(peak)?;
    a.peak = a.peak.max(add(a.bytes, peak)?);
    Ok(())
}
// Own the child ledger across success and refusal; every consumed phase and
// admitted peak is folded into the cumulative fitter receipt exactly once.
fn run_selector<T>(
    a: &mut Admission,
    f: impl FnOnce(&mut Admission) -> Result<T, Error>,
) -> Result<T, Error> {
    let mut child = Admission::new(remaining(a)?);
    let result = f(&mut child);
    absorb(a, child.work, child.memory(0))?;
    result
}
fn admitted_model<'a>(
    d: usize,
    s: usize,
    parameters: &'a [f32],
    a: &mut Admission,
) -> Result<Model<'a>, Error> {
    a.charge(add(mul(parameters.len(), 5)?, 30)? as u64)?;
    Model::new(d, s, parameters)
}
fn membership<'a>(
    input: FitInput<'_>,
    side: usize,
    owners: &'a [u32],
    digests: &'a [Hash],
    a: &mut Admission,
) -> Result<Membership<'a>, Error> {
    let m = run_selector(a, |child| {
        Membership::new_admitted(input.dimension, side, input.rows, owners, digests, child)
    })?;
    a.account(m.storage().owned_capacity_bytes)?;
    Ok(m)
}
fn caches(
    model: &Model<'_>,
    training: &PreparedTraining<'_>,
    a: &mut Admission,
) -> Result<Vec<PreparedQuery>, Error> {
    let mut cache = a.vector::<PreparedQuery>(training.observations().len())?;
    for observation in training.observations() {
        let p = run_selector(a, |child| {
            selector::prepare_observed_query_admitted(model, observation, child)
        })?;
        a.account(p.memory().retained_capacity_bytes)?;
        cache.push(p);
    }
    Ok(cache)
}
fn cache_bytes(cache: &Vec<PreparedQuery>) -> Result<usize, Error> {
    cache.iter().try_fold(capacity(cache)?, |b, p| {
        add(b, p.memory().retained_capacity_bytes)
    })
}
fn coverage(
    input: FitInput<'_>,
    model: &Model<'_>,
    m: &Membership<'_>,
    training: &PreparedTraining<'_>,
    cache: &[PreparedQuery],
    a: &mut Admission,
) -> Result<PreparedCoverage, Error> {
    run_selector(a, |child| {
        selector::evaluate_observed_admitted(
            model,
            m,
            training,
            cache,
            input.snapshot,
            input.budget,
            child,
        )
    })
}
fn compare(
    before: &PreparedCoverage,
    after: &PreparedCoverage,
    a: &mut Admission,
) -> Result<Acceptance, Error> {
    let receipt = selector::compare_coverage(before, after, remaining(a)?)?;
    absorb(a, receipt.work.operations, receipt.memory)?;
    Ok(receipt)
}

fn guard(keep_going: &mut impl FnMut(Progress) -> bool, progress: Progress) -> Result<(), Error> {
    if keep_going(progress) {
        Ok(())
    } else {
        Err(Error::Refused("caller stopped incomplete fitting"))
    }
}
fn checked_f64(x: f64) -> Result<f64, Error> {
    if x.is_finite() {
        Ok(x)
    } else {
        Err(Error::Invalid("nonfinite gradient/update"))
    }
}

// All offsets follow the native parameter order; there is no optimizer state.
fn offsets(d: usize, s: usize) -> (usize, usize, usize, usize, usize, usize, usize) {
    let hb = HIDDEN * d;
    let mw = hb + HIDDEN;
    let mb = mw + MIXTURES * HIDDEN;
    let uw = mb + MIXTURES;
    let ub = uw + MIXTURES * s * HIDDEN;
    let vw = ub + MIXTURES * s;
    let vb = vw + MIXTURES * s * HIDDEN;
    (hb, mw, mb, uw, ub, vw, vb)
}
fn initialize(d: usize, s: usize, source: Hash, a: &mut Admission) -> Result<Vec<f32>, Error> {
    let count = parameter_count(d, s)?;
    a.charge(mul(count, 96)? as u64)?;
    let mut parameters = a.vector::<f32>(count)?;
    let (hb, _, mb, _, ub, _, vb) = offsets(d, s);
    for i in 0..count {
        let mut h = Sha256::new();
        h.update(b"BORSUK-budget-fitter-init-v1");
        h.update(SEED.to_le_bytes());
        h.update(source);
        h.update((i as u64).to_le_bytes());
        let bytes: Hash = h.finalize().into();
        let centered = i64::from(u32::from_le_bytes(
            bytes[..4]
                .try_into()
                .map_err(|_| Error::Invalid("SHA initialization"))?,
        )) - (1i64 << 31);
        let value = if (hb..hb + HIDDEN).contains(&i) {
            1. / 256.
        } else if (mb..mb + MIXTURES).contains(&i)
            || (ub..ub + MIXTURES * s).contains(&i)
            || i >= vb
        {
            0.
        } else {
            (centered as f64 / (1u64 << 39) as f64) as f32
        };
        parameters.push(value);
    }
    Ok(parameters)
}

/// Forward arithmetic is sequential f32, exactly as the serving selector.
fn forward(
    parameters: &[f32],
    d: usize,
    s: usize,
    input: &[f32],
    probabilities: &mut [f32],
) -> Result<[f32; HIDDEN], Error> {
    let (hb, mw, mb, uw, ub, vw, vb) = offsets(d, s);
    let mut hidden = [0f32; HIDDEN];
    for (h, value) in hidden.iter_mut().enumerate() {
        *value = linear(&parameters[h * d..(h + 1) * d], input, parameters[hb + h])?.max(0.);
    }
    for r in 0..MIXTURES {
        probabilities[r] = linear(
            &parameters[mw + r * HIDDEN..mw + (r + 1) * HIDDEN],
            &hidden,
            parameters[mb + r],
        )?;
    }
    softmax(&mut probabilities[..MIXTURES])?;
    for (w, b, start) in [(uw, ub, MIXTURES), (vw, vb, MIXTURES + MIXTURES * s)] {
        for r in 0..MIXTURES {
            for i in 0..s {
                let row = r * s + i;
                probabilities[start + row] = linear(
                    &parameters[w + row * HIDDEN..w + (row + 1) * HIDDEN],
                    &hidden,
                    parameters[b + row],
                )?;
            }
            softmax(&mut probabilities[start + r * s..start + (r + 1) * s])?;
        }
    }
    Ok(hidden)
}

#[allow(clippy::too_many_arguments)]
fn backward(
    parameters: &[f32],
    d: usize,
    s: usize,
    input: &[f32],
    hidden: &[f32; HIDDEN],
    p: &[f32],
    neighbors: &[selector::GroupWeight],
    owners: &[u32],
    mass: f64,
    gradient: &mut [f64],
    delta: &mut [f64],
    label_mass: &mut [u16],
) -> Result<f64, Error> {
    delta.fill(0.);
    label_mass.fill(0);
    for w in neighbors {
        let label = owners[w.group as usize] as usize;
        label_mass[label] = label_mass[label]
            .checked_add(w.weight)
            .ok_or(Error::Overflow)?;
    }
    let mut loss = 0f64;
    for (label, &count) in label_mass.iter().enumerate() {
        if count == 0 {
            continue;
        }
        let i = label / s;
        let j = label % s;
        let probability = f64::from(selector::model_score(p, s, label as u32)?);
        let y = f64::from(count) / mass;
        loss = checked_f64(loss - y * (probability + EPSILON).ln())?;
        let derivative = -y / (probability + EPSILON);
        for r in 0..MIXTURES {
            let u = MIXTURES + r * s + i;
            let v = MIXTURES + MIXTURES * s + r * s + j;
            delta[r] += derivative * f64::from(p[u]) * f64::from(p[v]);
            delta[u] += derivative * f64::from(p[r]) * f64::from(p[v]);
            delta[v] += derivative * f64::from(p[r]) * f64::from(p[u]);
        }
    }
    // Softmax Jacobians, followed by all linear rows in parameter order.
    let mut jacobian = |start: usize, len: usize| -> Result<(), Error> {
        let mut dot = 0f64;
        for k in start..start + len {
            dot = checked_f64(dot + f64::from(p[k]) * delta[k])?;
        }
        for k in start..start + len {
            delta[k] = checked_f64(f64::from(p[k]) * (delta[k] - dot))?;
        }
        Ok(())
    };
    jacobian(0, MIXTURES)?;
    for head in 0..2 {
        for r in 0..MIXTURES {
            jacobian(MIXTURES + head * MIXTURES * s + r * s, s)?;
        }
    }
    let (hb, mw, mb, uw, ub, vw, vb) = offsets(d, s);
    let mut hidden_gradient = [0f64; HIDDEN];
    for (w, b, start, len) in [
        (mw, mb, 0, MIXTURES),
        (uw, ub, MIXTURES, MIXTURES * s),
        (vw, vb, MIXTURES + MIXTURES * s, MIXTURES * s),
    ] {
        for row in 0..len {
            let derivative = delta[start + row];
            gradient[b + row] = checked_f64(gradient[b + row] + derivative)?;
            for h in 0..HIDDEN {
                gradient[w + row * HIDDEN + h] = checked_f64(
                    gradient[w + row * HIDDEN + h] + derivative * f64::from(hidden[h]),
                )?;
                hidden_gradient[h] = checked_f64(
                    hidden_gradient[h] + derivative * f64::from(parameters[w + row * HIDDEN + h]),
                )?;
            }
        }
    }
    for h in 0..HIDDEN {
        // ReLU derivative at zero is zero.
        let derivative = if hidden[h] > 0. {
            hidden_gradient[h]
        } else {
            0.
        };
        gradient[hb + h] = checked_f64(gradient[hb + h] + derivative)?;
        for x in 0..d {
            gradient[h * d + x] =
                checked_f64(gradient[h * d + x] + derivative * f64::from(input[x]))?;
        }
    }
    Ok(loss)
}

#[allow(clippy::too_many_arguments)]
fn train(
    input: FitInput<'_>,
    s: usize,
    parameters: &mut [f32],
    owners: &[u32],
    normalized: &PreparedTraining<'_>,
    mass: f64,
    round: usize,
    a: &mut Admission,
    keep_going: &mut impl FnMut(Progress) -> bool,
) -> Result<(), Error> {
    let mut gradient = a.vector::<f64>(parameters.len())?;
    gradient.resize(parameters.len(), 0.);
    let count = MIXTURES + 2 * MIXTURES * s;
    let mut probabilities = a.vector::<f32>(count)?;
    probabilities.resize(count, 0.);
    let mut delta = a.vector::<f64>(count)?;
    delta.resize(count, 0.);
    let mut label_mass = a.vector::<u16>(mul(s, s)?)?;
    label_mass.resize(s * s, 0);
    for epoch in 0..EPOCHS {
        for (batch, samples) in input.anchors.chunks(BATCH).enumerate() {
            guard(
                keep_going,
                Progress {
                    round,
                    epoch,
                    batch,
                    group: None,
                    operations: a.work,
                },
            )?;
            a.charge(parameters.len() as u64)?;
            gradient.fill(0.);
            for (local, sample) in samples.iter().enumerate() {
                // Conservative scalar multiply/add, finite checks, Jacobians,
                // target scoring and linear backprop, all before the work.
                a.charge(add(
                    mul(parameters.len(), 12)?,
                    add(
                        mul(sample.neighbors.len(), MIXTURES * 24)?,
                        mul(label_mass.len(), 2)?,
                    )?,
                )? as u64)?;
                let anchor = batch * BATCH + local;
                let query = normalized.observations()[anchor].normalized_query();
                let hidden = forward(parameters, input.dimension, s, query, &mut probabilities)?;
                backward(
                    parameters,
                    input.dimension,
                    s,
                    query,
                    &hidden,
                    &probabilities,
                    sample.neighbors,
                    owners,
                    mass,
                    &mut gradient,
                    &mut delta,
                    &mut label_mass,
                )?;
            }
            a.charge(mul(parameters.len(), 5)? as u64)?;
            for (p, g) in parameters.iter_mut().zip(&gradient) {
                *p = finite(
                    checked_f64(f64::from(*p) - RATE * (*g / samples.len() as f64))? as f32,
                )?;
            }
        }
    }
    a.release(add(
        capacity(&gradient)?,
        add(
            add(capacity(&probabilities)?, capacity(&delta)?)?,
            capacity(&label_mass)?,
        )?,
    )?);
    Ok(())
}

fn diagnostic_tokens(
    input: FitInput<'_>,
    side: usize,
    owners: &[u32],
    tokens: &mut [Hash],
    a: &mut Admission,
) -> Result<(), Error> {
    a.charge(add(add(mul(tokens.len(), 160)?, mul(owners.len(), 20)?)?, 96)? as u64)?;
    // Bind source and the complete row/group layout. These synthetic diagnostics
    // deliberately have a different domain from authenticated payload hashes.
    let mut layout = Sha256::new();
    layout.update(b"BORSUK-budget-fitter-layout-v1");
    layout.update(input.source);
    layout.update((input.dimension as u32).to_le_bytes());
    layout.update((side as u32).to_le_bytes());
    layout.update(input.rows.to_le_bytes());
    layout.update((owners.len() as u64).to_le_bytes());
    for owner in owners {
        layout.update(owner.to_le_bytes());
    }
    let layout: Hash = layout.finalize().into();
    tokens.fill([0; 32]);
    for &label in owners {
        if tokens[label as usize] == [0; 32] {
            let mut h = Sha256::new();
            h.update(b"BORSUK-budget-fitter-offline-body-v1");
            h.update(input.source);
            h.update(layout);
            h.update(label.to_le_bytes());
            let mut token: Hash = h.finalize().into();
            // Unique/nonzero within this grid without relying on collisions.
            token[..4].copy_from_slice(&(label + 1).to_le_bytes());
            tokens[label as usize] = token;
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum MoveKind {
    Move,
    Swap(u32),
    CapacityRefused,
}
fn propose(
    old: &Membership<'_>,
    group: usize,
    destination: u32,
    owners: &mut [u32],
    a: &mut Admission,
) -> Result<MoveKind, Error> {
    if group >= old.owners().len()
        || destination as usize >= old.objects().len()
        || owners.len() != old.owners().len()
    {
        return Err(Error::Invalid("proposal geometry"));
    }
    a.charge(mul(owners.len(), 3)? as u64)?;
    owners.copy_from_slice(old.owners());
    let source = owners[group];
    if destination == source {
        return Err(Error::Invalid("unchanged proposal"));
    }
    let count = owners.len();
    let rows = |g: usize| {
        if g + 1 == count && old.total_rows() % 16 != 0 {
            (old.total_rows() % 16) as i32
        } else {
            16
        }
    };
    let group_rows = rows(group);
    let object = old.objects()[destination as usize];
    if object.groups < 64 && i32::from(object.rows) + group_rows <= 1024 {
        owners[group] = destination;
        return Ok(MoveKind::Move);
    }
    let src = old.objects()[source as usize];
    for (other, &owner) in old.owners().iter().enumerate() {
        if owner == destination
            && i32::from(object.rows) - rows(other) + group_rows <= 1024
            && i32::from(src.rows) - group_rows + rows(other) <= 1024
        {
            owners[other] = source;
            owners[group] = destination;
            return Ok(MoveKind::Swap(other as u32));
        }
    }
    Ok(MoveKind::CapacityRefused)
}

fn destinations(
    input: FitInput<'_>,
    cache: &[PreparedQuery],
    group: usize,
    current_owner: u32,
    aggregate: &mut [f64],
    a: &mut Admission,
) -> Result<Option<[u32; 8]>, Error> {
    a.charge(add(aggregate.len(), mul(input.anchors.len(), 100)?)? as u64)?;
    aggregate.fill(0.);
    let s = cache
        .first()
        .map(|p| p.probabilities().len())
        .ok_or(Error::Invalid("empty fitting inference"))?;
    let s = (s - MIXTURES) / (2 * MIXTURES);
    let mut has_teacher = false;
    for (sample, p) in input.anchors.iter().zip(cache) {
        if let Some(w) = sample.neighbors.iter().find(|w| w.group as usize == group) {
            has_teacher = true;
            a.charge(mul(aggregate.len(), MIXTURES * 3 + 2)? as u64)?;
            for (label, score) in aggregate.iter_mut().enumerate() {
                *score = checked_f64(
                    *score
                        + f64::from(w.weight)
                            * f64::from(selector::model_score(p.probabilities(), s, label as u32)?),
                )?;
            }
        }
    }
    if !has_teacher {
        return Ok(None);
    }
    a.charge(mul(aggregate.len(), 8 * 4)? as u64)?;
    let mut top = [u32::MAX; 8];
    for label in 0..aggregate.len() {
        if label as u32 == current_owner {
            continue;
        }
        for slot in 0..8 {
            let old = top[slot];
            if old == u32::MAX
                || aggregate[label] > aggregate[old as usize]
                || (aggregate[label] == aggregate[old as usize] && label < (old as usize))
            {
                top.copy_within(slot..7, slot + 1);
                top[slot] = label as u32;
                break;
            }
        }
    }
    Ok(Some(top))
}

/// Fixed source-only method. Limits may tighten, never exceed frozen caps.
/// Capacity admission includes borrowed anchor slices alongside all owned
/// model/checkpoint/cache/membership/gradient buffers and declared fixed arrays.
/// Destination scores sum teacher-count-weighted probabilities in anchor order
/// over every grid label (including empty labels), with label-ascending ties.
/// At each ascending group visit, exclude its then-current owner before top8.
/// Traverse that frozen destination list in rank order, accepting each strict
/// gain against the current accepted state; continue after acceptance. A later
/// destination equal to the current owner is skipped. Full destinations use
/// the smallest ordinal donor that preserves both capacities, without ranking
/// swaps by loss. Every candidate evaluates all anchors, including nonteachers.
/// A completed result can contain no accepted gain; the receipts state this.
/// `keep_going` is a pure caller admission/deadline callback; false refuses the
/// entire incomplete computation. Source slices are never mutated.
pub fn fit(
    input: FitInput<'_>,
    limits: Limits,
    keep_going: impl FnMut(Progress) -> bool,
) -> Result<FitResult, FitFailure> {
    fit_with_mass(input, limits, keep_going, 100.)
}
fn fit_with_mass(
    input: FitInput<'_>,
    limits: Limits,
    mut keep_going: impl FnMut(Progress) -> bool,
    mass: f64,
) -> Result<FitResult, FitFailure> {
    let mut a = Admission::new(limits);
    let mut evidence = FitEvidence {
        stage: FitStage::Admission,
        accepted: None,
    };
    fit_inner(input, &mut a, &mut keep_going, mass, &mut evidence).map_err(|error| FitFailure {
        error,
        work: Work { operations: a.work },
        memory: a.memory(0),
        completed: false,
        stage: evidence.stage,
        accepted: evidence.accepted,
    })
}
fn fit_inner(
    input: FitInput<'_>,
    a: &mut Admission,
    keep_going: &mut impl FnMut(Progress) -> bool,
    mass: f64,
    evidence: &mut FitEvidence,
) -> Result<FitResult, Error> {
    if input.source == [0; 32]
        || !(1..=100_000).contains(&input.rows)
        || input.anchors.is_empty()
        || input.anchors.len() > 256
    {
        return Err(Error::Invalid("fitting source/population"));
    }
    if a.limits.operations > MAX_OPERATIONS || a.limits.transient_bytes > MAX_CAPACITY_BYTES {
        return Err(Error::Invalid("frozen fitting cap"));
    }
    input.snapshot.validate()?;
    input.budget.reservations()?;
    let groups = usize::try_from(input.rows.div_ceil(16)).map_err(|_| Error::Overflow)?;
    let labels_needed = groups.div_ceil(56);
    let mut s = 1usize;
    while mul(s, s)? < labels_needed {
        s += 1;
    }
    let labels = mul(s, s)?;
    let count = parameter_count(input.dimension, s)?;
    a.charge(80)?;
    // Explicit fixed arrays used by forward/backward and top-eight proposal.
    a.fixed(HIDDEN * (size_of::<f32>() + size_of::<f64>()) + 8 * size_of::<u32>())?;
    guard(
        keep_going,
        Progress {
            round: 0,
            epoch: 0,
            batch: 0,
            group: None,
            operations: a.work,
        },
    )?;
    a.charge(input.anchors.len() as u64)?;
    let mut borrowed_input_bytes = mul(input.anchors.len(), size_of::<TrainingSample<'_>>())?;
    for sample in input.anchors {
        if sample.query.len() != input.dimension || sample.neighbors.len() > 100 {
            return Err(Error::Invalid("fitting input shape"));
        }
        borrowed_input_bytes = add(
            borrowed_input_bytes,
            add(
                mul(sample.query.len(), size_of::<f32>())?,
                mul(sample.neighbors.len(), size_of::<selector::GroupWeight>())?,
            )?,
        )?;
    }
    a.account(borrowed_input_bytes)?;
    // Production mass is a fitting constraint. Query identity/normalization
    // and unique positive group/row validation happen once in prepare_training.
    for sample in input.anchors {
        a.charge(sample.neighbors.len() as u64)?;
        let total = sample
            .neighbors
            .iter()
            .try_fold(0u64, |total, w| sum(total, u64::from(w.weight)))?;
        if total as f64 != mass {
            return Err(Error::Invalid("source-neighbor mass"));
        }
    }
    evidence.stage = FitStage::Initialization;
    let mut owners = a.vector::<u32>(groups)?;
    for group in 0..groups {
        owners.push((group / 56) as u32);
    }
    let mut digests = a.vector::<Hash>(labels)?;
    digests.resize(labels, [0; 32]);
    diagnostic_tokens(input, s, &owners, &mut digests, a)?;
    let mut parameters = initialize(input.dimension, s, input.source, a)?;
    let model = admitted_model(input.dimension, s, &parameters, a)?;
    let initial_membership = membership(input, s, &owners, &digests, a)?;
    evidence.stage = FitStage::ObservationPreparation;
    let observations = run_selector(a, |child| {
        selector::prepare_training_admitted(&model, &initial_membership, input.anchors, child)
    })?;
    a.account(observations.memory().retained_capacity_bytes)?;
    let training = observations.sha256();
    evidence.accepted = Some(FitIdentity {
        source: input.source,
        training,
        model: model.sha256(),
        membership: initial_membership.sha256(),
        snapshot: input.snapshot,
        budget: input.budget,
    });
    evidence.stage = FitStage::InitialCoverage;
    let mut cache = caches(&model, &observations, a)?;
    // Accepted and proposed coverage summaries coexist. Actual serving charges
    // remain semantic data; previously performed evaluations are not charged again.
    a.fixed(2 * size_of::<PreparedCoverage>())?;
    let mut baseline = coverage(input, &model, &initial_membership, &observations, &cache, a)?;
    if !baseline.complete() {
        return Err(Error::Refused("initial incomplete serving coverage"));
    }
    a.release(initial_membership.storage().owned_capacity_bytes);
    drop(initial_membership);
    // Worst-case receipt count is admitted before any optimizer mutation.
    let event_limit = mul(ROUNDS, add(1, mul(groups, 8)?)?)?;
    let mut events = a.vector::<FitEvent>(event_limit)?;
    let mut candidate_owners = a.vector::<u32>(groups)?;
    candidate_owners.extend_from_slice(&owners);
    let mut candidate_digests = a.vector::<Hash>(labels)?;
    candidate_digests.resize(labels, [0; 32]);
    let mut aggregate = a.vector::<f64>(labels)?;
    aggregate.resize(labels, 0.);
    let mut candidate_parameters = a.vector::<f32>(count)?;
    candidate_parameters.extend_from_slice(&parameters);
    for round in 0..ROUNDS {
        evidence.stage = FitStage::Training { round };
        candidate_parameters.copy_from_slice(&parameters);
        train(
            input,
            s,
            &mut candidate_parameters,
            &owners,
            &observations,
            mass,
            round,
            a,
            keep_going,
        )?;
        evidence.stage = FitStage::Checkpoint { round };
        let before = admitted_model(input.dimension, s, &parameters, a)?;
        let after = admitted_model(input.dimension, s, &candidate_parameters, a)?;
        guard(
            keep_going,
            Progress {
                round,
                epoch: EPOCHS,
                batch: 0,
                group: None,
                operations: a.work,
            },
        )?;
        let candidate_cache = caches(&after, &observations, a)?;
        let m = membership(input, s, &owners, &digests, a)?;
        a.charge(8)?;
        baseline.validate(&before, &m, input.snapshot, &observations, input.budget)?;
        let proposed = coverage(input, &after, &m, &observations, &candidate_cache, a)?;
        guard(
            keep_going,
            Progress {
                round,
                epoch: EPOCHS,
                batch: 0,
                group: None,
                operations: a.work,
            },
        )?;
        let receipt = compare(&baseline, &proposed, a)?;
        let accept = receipt.accepted();
        if accept {
            let identity = evidence
                .accepted
                .as_mut()
                .ok_or(Error::Invalid("missing accepted identity"))?;
            identity.model = receipt.after_model;
            identity.membership = receipt.after_membership;
        }
        a.release(m.storage().owned_capacity_bytes);
        drop(m);
        events.push(FitEvent::Checkpoint { round, receipt });
        if accept {
            std::mem::swap(&mut parameters, &mut candidate_parameters);
            a.release(cache_bytes(&cache)?);
            cache = candidate_cache;
            baseline = proposed;
        } else {
            a.release(cache_bytes(&candidate_cache)?);
        }
        let model = admitted_model(input.dimension, s, &parameters, a)?;
        for group in 0..groups {
            evidence.stage = FitStage::Destinations {
                round,
                group: group as u32,
            };
            guard(
                keep_going,
                Progress {
                    round,
                    epoch: EPOCHS,
                    batch: 0,
                    group: Some(group as u32),
                    operations: a.work,
                },
            )?;
            let Some(top) = destinations(input, &cache, group, owners[group], &mut aggregate, a)?
            else {
                events.push(FitEvent::NoTeacher {
                    round,
                    group: group as u32,
                });
                continue;
            };
            for &destination in &top[..(labels - 1).min(8)] {
                if destination == owners[group] {
                    continue;
                }
                evidence.stage = FitStage::Proposal {
                    round,
                    group: group as u32,
                    destination,
                };
                guard(
                    keep_going,
                    Progress {
                        round,
                        epoch: EPOCHS,
                        batch: 0,
                        group: Some(group as u32),
                        operations: a.work,
                    },
                )?;
                let old = membership(input, s, &owners, &digests, a)?;
                let swap = match propose(&old, group, destination, &mut candidate_owners, a)? {
                    MoveKind::Move => None,
                    MoveKind::Swap(other) => Some(other),
                    MoveKind::CapacityRefused => {
                        events.push(FitEvent::CapacityRefused {
                            round,
                            group: group as u32,
                            destination,
                        });
                        a.release(old.storage().owned_capacity_bytes);
                        continue;
                    }
                };
                diagnostic_tokens(input, s, &candidate_owners, &mut candidate_digests, a)?;
                let new = membership(input, s, &candidate_owners, &candidate_digests, a)?;
                a.charge(8)?;
                baseline.validate(&model, &old, input.snapshot, &observations, input.budget)?;
                let proposed = coverage(input, &model, &new, &observations, &cache, a)?;
                guard(
                    keep_going,
                    Progress {
                        round,
                        epoch: EPOCHS,
                        batch: 0,
                        group: Some(group as u32),
                        operations: a.work,
                    },
                )?;
                let receipt = compare(&baseline, &proposed, a)?;
                let accept = receipt.accepted();
                if accept {
                    let identity = evidence
                        .accepted
                        .as_mut()
                        .ok_or(Error::Invalid("missing accepted identity"))?;
                    identity.model = receipt.after_model;
                    identity.membership = receipt.after_membership;
                }
                a.release(add(
                    old.storage().owned_capacity_bytes,
                    new.storage().owned_capacity_bytes,
                )?);
                drop((old, new));
                events.push(FitEvent::Proposal {
                    round,
                    group: group as u32,
                    destination,
                    swap,
                    receipt,
                });
                if accept {
                    std::mem::swap(&mut owners, &mut candidate_owners);
                    std::mem::swap(&mut digests, &mut candidate_digests);
                    baseline = proposed;
                }
            }
        }
    }
    evidence.stage = FitStage::Finalization;
    let model = admitted_model(input.dimension, s, &parameters, a)?;
    let final_membership = membership(input, s, &owners, &digests, a)?;
    let membership = final_membership.sha256();
    a.release(final_membership.storage().owned_capacity_bytes);
    drop(final_membership);
    let model_hash = model.sha256();
    let retained = add(
        add(capacity(&parameters)?, capacity(&owners)?)?,
        add(capacity(&digests)?, capacity(&events)?)?,
    )?;
    a.release(cache_bytes(&cache)?);
    drop(cache);
    a.release(add(
        add(
            capacity(&candidate_parameters)?,
            capacity(&candidate_owners)?,
        )?,
        add(
            add(
                capacity(&candidate_digests)?,
                observations.memory().retained_capacity_bytes,
            )?,
            capacity(&aggregate)?,
        )?,
    )?);
    drop((
        candidate_parameters,
        candidate_owners,
        candidate_digests,
        observations,
        aggregate,
    ));
    a.release(borrowed_input_bytes);
    debug_assert_eq!(a.bytes, retained);
    Ok(FitResult {
        source: input.source,
        training,
        snapshot: input.snapshot,
        budget: input.budget,
        model: model_hash,
        membership,
        parameters,
        owners,
        diagnostic_tokens: digests,
        side: s,
        events,
        work: Work { operations: a.work },
        memory: a.memory(retained),
        borrowed_input_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use selector::{AcceptanceDecision, Charge, GroupWeight, Refusal};

    fn limits() -> Limits {
        Limits {
            transient_bytes: 16 * 1024 * 1024,
            operations: MAX_OPERATIONS,
        }
    }
    fn input<'a>(rows: u64, anchors: &'a [TrainingSample<'a>]) -> FitInput<'a> {
        FitInput {
            source: [7; 32],
            dimension: 1,
            rows,
            anchors,
            snapshot: Snapshot {
                root: [1; 32],
                coefficients: [2; 32],
                delta: [3; 32],
                revision: 1,
            },
            budget: Budget {
                max_reads: 1,
                max_bytes: selector::max_body_reservation_bytes(1).unwrap(),
                head: Charge::ZERO,
                root: Charge::ZERO,
                delta: Charge::ZERO,
                attempts: Charge::ZERO,
            },
        }
    }
    // Separate fixture-only seam: smaller positive populations never alter the
    // production 100-neighbor target, initialization, epochs, batches or rounds.
    fn tiny(
        input: FitInput<'_>,
        limits: Limits,
        guard: impl FnMut(Progress) -> bool,
    ) -> Result<FitResult, FitFailure> {
        fit_with_mass(input, limits, guard, 1.)
    }

    // Independent layout, sequential scalar forward and analytical gradient.
    // Expected values do not call production offsets/forward/backward/scoring.
    fn scalar_oracle(
        parameters: &[f32],
        raw: &[f32],
        side: usize,
        label: usize,
    ) -> (Vec<f32>, Vec<f64>, f64) {
        let d = raw.len();
        let norm = raw
            .iter()
            .map(|x| f64::from(*x) * f64::from(*x))
            .sum::<f64>()
            .sqrt();
        let x: Vec<f32> = raw.iter().map(|v| (f64::from(*v) / norm) as f32).collect();
        let mut cursor = 0;
        let hw = cursor;
        cursor += 64 * d;
        let hb = cursor;
        cursor += 64;
        let mw = cursor;
        cursor += 4 * 64;
        let mb = cursor;
        cursor += 4;
        let uw = cursor;
        cursor += 4 * side * 64;
        let ub = cursor;
        cursor += 4 * side;
        let vw = cursor;
        cursor += 4 * side * 64;
        let vb = cursor;
        let mut hidden = [0f32; 64];
        for h in 0..64 {
            let mut v = 0f32;
            for j in 0..d {
                v += parameters[hw + h * d + j] * x[j];
            }
            hidden[h] = (v + parameters[hb + h]).max(0.);
        }
        let mut out = vec![0f32; 4 + 8 * side];
        for (w, b, start, n) in [
            (mw, mb, 0, 4),
            (uw, ub, 4, 4 * side),
            (vw, vb, 4 + 4 * side, 4 * side),
        ] {
            for row in 0..n {
                let mut v = 0f32;
                for h in 0..64 {
                    v += parameters[w + row * 64 + h] * hidden[h];
                }
                out[start + row] = v + parameters[b + row];
            }
        }
        for (start, n) in std::iter::once((0, 4)).chain((0..8).map(|r| (4 + r * side, side))) {
            let max = out[start..start + n]
                .iter()
                .copied()
                .fold(f32::NEG_INFINITY, f32::max);
            let mut divisor = 0f32;
            for v in &mut out[start..start + n] {
                *v = (*v - max).exp();
                divisor += *v;
            }
            for v in &mut out[start..start + n] {
                *v /= divisor;
            }
        }
        let i = label / side;
        let j = label % side;
        let mut probability = 0f32;
        for r in 0..4 {
            probability += (out[r] * out[4 + r * side + i]) * out[4 + 4 * side + r * side + j];
        }
        let epsilon = 2f64.powi(-24);
        let loss = -(f64::from(probability) + epsilon).ln();
        // Each component contributes a responsibility. Subtracting each head's
        // expected derivative independently implements the softmax Jacobian.
        let mut logits = vec![0f64; out.len()];
        let denom = f64::from(probability) + epsilon;
        let mut responsibility = [0f64; 4];
        for r in 0..4 {
            responsibility[r] = f64::from(out[r])
                * f64::from(out[4 + r * side + i])
                * f64::from(out[4 + 4 * side + r * side + j])
                / denom;
        }
        let total = responsibility.iter().sum::<f64>();
        for r in 0..4 {
            logits[r] = f64::from(out[r]) * total - responsibility[r];
            for coordinate in 0..side {
                logits[4 + r * side + coordinate] = responsibility[r]
                    * (f64::from(out[4 + r * side + coordinate])
                        - if coordinate == i { 1. } else { 0. });
                logits[4 + 4 * side + r * side + coordinate] = responsibility[r]
                    * (f64::from(out[4 + 4 * side + r * side + coordinate])
                        - if coordinate == j { 1. } else { 0. });
            }
        }
        let mut g = vec![0f64; parameters.len()];
        for h in 0..64 {
            let mut dh = 0f64;
            for (w, b, start, n) in [
                (mw, mb, 0, 4),
                (uw, ub, 4, 4 * side),
                (vw, vb, 4 + 4 * side, 4 * side),
            ] {
                for row in 0..n {
                    g[w + row * 64 + h] = logits[start + row] * f64::from(hidden[h]);
                    g[b + row] = logits[start + row];
                    dh += logits[start + row] * f64::from(parameters[w + row * 64 + h]);
                }
            }
            if hidden[h] <= 0. {
                dh = 0.;
            }
            g[hb + h] = dh;
            for coordinate in 0..d {
                g[hw + h * d + coordinate] = dh * f64::from(x[coordinate]);
            }
        }
        (out, g, loss)
    }
    fn independent_initial(d: usize, s: usize) -> Vec<f32> {
        let hb = 64 * d;
        let mb = hb + 64 + 4 * 64;
        let ub = mb + 4 + 4 * s * 64;
        let vb = ub + 4 * s + 4 * s * 64;
        (0..64 * (d + 1) + 4 * 65 + 8 * s * 65)
            .map(|i| {
                if (hb..hb + 64).contains(&i) {
                    return 1. / 256.;
                }
                if (mb..mb + 4).contains(&i) || (ub..ub + 4 * s).contains(&i) || i >= vb {
                    return 0.;
                }
                let mut hash = Sha256::new();
                hash.update(b"BORSUK-budget-fitter-init-v1");
                hash.update(20260923u64.to_le_bytes());
                hash.update([7; 32]);
                hash.update((i as u64).to_le_bytes());
                let bytes = hash.finalize();
                let word = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                ((f64::from(word) - 2147483648.) / 549755813888.) as f32
            })
            .collect()
    }
    #[test]
    fn oracle_fitter_asymmetric_forward_gradient_and_finite_difference() {
        let d = 3;
        let s = 2;
        let mut parameters = vec![0f32; 64 * (d + 1) + 4 * 65 + 8 * s * 65];
        for (i, v) in parameters.iter_mut().enumerate() {
            *v = ((i * 17 % 29) as f32 - 14.) / 128.;
        }
        for h in 0..64 {
            parameters[64 * d + h] = if h % 3 == 0 { -1. } else { 0.25 };
        }
        parameters[63 * d..64 * d].fill(0.);
        parameters[64 * d + 63] = 0.;
        let raw = [3f32, -4., 12.];
        let query: Vec<f32> = raw.iter().map(|x| *x / 13.).collect();
        let (expected, expected_gradient, expected_loss) = scalar_oracle(&parameters, &raw, s, 3);
        let mut probability = vec![0f32; 4 + 8 * s];
        let hidden = forward(&parameters, d, s, &query, &mut probability).unwrap();
        assert_eq!(
            probability.iter().map(|p| p.to_bits()).collect::<Vec<_>>(),
            expected.iter().map(|p| p.to_bits()).collect::<Vec<_>>()
        );
        let model = Model::new(d, s, &parameters).unwrap();
        let prepared = selector::prepare_query(&model, &raw, limits()).unwrap();
        assert_eq!(
            prepared
                .probabilities()
                .iter()
                .map(|p| p.to_bits())
                .collect::<Vec<_>>(),
            expected.iter().map(|p| p.to_bits()).collect::<Vec<_>>()
        );
        let mut gradient = vec![0f64; parameters.len()];
        let mut delta = vec![0f64; probability.len()];
        let loss = backward(
            &parameters,
            d,
            s,
            &query,
            &hidden,
            &probability,
            &[GroupWeight {
                group: 0,
                weight: 1,
            }],
            &[3],
            1.,
            &mut gradient,
            &mut delta,
            &mut vec![0; s * s],
        )
        .unwrap();
        assert_eq!(loss, expected_loss);
        for (actual, expected) in gradient.iter().zip(&expected_gradient) {
            assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
        }
        assert_eq!(hidden[0], 0.); // negative preactivation
        assert!(hidden[1] > 0.);
        assert_eq!(hidden[63], 0.); // exactly zero preactivation
        assert_eq!(gradient[64 * d + 63], 0.);
        assert!(gradient[63 * d..64 * d].iter().all(|g| *g == 0.));
        // f32 forward has rounding; use a finite step safely above one ulp.
        for index in [
            0,
            64 * d + 1,
            64 * (d + 1),
            64 * (d + 1) + 4 * 64 + 1,
            64 * (d + 1) + 4 * 65 + 1,
            parameters.len() - 1,
        ] {
            let mut plus = parameters.clone();
            plus[index] += 0.001;
            let mut minus = parameters.clone();
            minus[index] -= 0.001;
            let measured =
                (scalar_oracle(&plus, &raw, s, 3).2 - scalar_oracle(&minus, &raw, s, 3).2) / 0.002;
            assert!(
                (gradient[index] - measured).abs() < 0.001,
                "index {index}: {} != {measured}",
                gradient[index]
            );
        }
        // A probability far below epsilon exposes a p-only derivative bug.
        probability.fill(0.);
        probability[..4].fill(0.25);
        for r in 0..4 {
            probability[4 + r * s] = 1.;
            probability[4 + r * s + 1] = 1e-10;
            probability[4 + 4 * s + r * s] = 1.;
            probability[4 + 4 * s + r * s + 1] = 1e-10;
        }
        gradient.fill(0.);
        let tiny_loss = backward(
            &parameters,
            d,
            s,
            &query,
            &hidden,
            &probability,
            &[GroupWeight {
                group: 0,
                weight: 1,
            }],
            &[3],
            1.,
            &mut gradient,
            &mut delta,
            &mut vec![0; s * s],
        )
        .unwrap();
        assert!((tiny_loss + 2f64.powi(-24).ln()).abs() < 1e-10);
        assert!(gradient.iter().all(|g| g.is_finite() && g.abs() < 1e-10));
        // Exact p=epsilon: the smoothed derivative is half the CE derivative.
        let head = 1f32 / 4096.;
        for r in 0..4 {
            probability[4 + r * s] = 1. - head;
            probability[4 + r * s + 1] = head;
            probability[4 + 4 * s + r * s] = 1. - head;
            probability[4 + 4 * s + r * s + 1] = head;
        }
        gradient.fill(0.);
        let loss = backward(
            &parameters,
            d,
            s,
            &query,
            &hidden,
            &probability,
            &[GroupWeight {
                group: 0,
                weight: 1,
            }],
            &[3],
            1.,
            &mut gradient,
            &mut delta,
            &mut vec![0; s * s],
        )
        .unwrap();
        assert_eq!(loss, -(2. * 2f64.powi(-24)).ln());
        assert_eq!(delta[0], 0.);
        assert_eq!(delta[5], -(1. - f64::from(head)) / 8.);
        assert_eq!(delta[4], (1. - f64::from(head)) / 8.);
    }
    #[test]
    fn oracle_fitter_sha_initialization_and_fixed_batch_update_order() {
        let mut a = Admission::new(limits());
        let d2 = initialize(2, 2, [7; 32], &mut a).unwrap();
        assert_eq!(d2[0].to_bits(), 0x3b1feafe);
        assert_eq!(d2[1].to_bits(), 0xb82a59ac);
        let initialized = initialize(1, 2, [7; 32], &mut a).unwrap();
        // Literal vectors independently generated from canonical SHA bytes.
        for (index, bits) in [
            (0, 0x3b1feafe),
            (63, 0x38a0169c),
            (128, 0x3a4468b7),
            (388, 0x3a8f74ac),
            (899, 0xbb44e20b),
            (908, 0xb9083a10),
            (1419, 0xba9d6430),
            (64, 0x3b800000),
            (384, 0),
            (900, 0),
            (1420, 0),
        ] {
            assert_eq!(initialized[index].to_bits(), bits, "parameter {index}");
        }
        for (i, &value) in initialized.iter().enumerate() {
            let is_hidden_bias = (64..128).contains(&i);
            let is_output_bias = (384..388).contains(&i) || (900..908).contains(&i) || i >= 1420;
            let expected = if is_hidden_bias {
                1. / 256.
            } else if is_output_bias {
                0.
            } else {
                let mut hash = Sha256::new();
                hash.update(b"BORSUK-budget-fitter-init-v1");
                hash.update(20260923u64.to_le_bytes());
                hash.update([7; 32]);
                hash.update((i as u64).to_le_bytes());
                let bytes = hash.finalize();
                let word = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                ((f64::from(word) - 2147483648.) / 549755813888.) as f32
            };
            assert_eq!(value.to_bits(), expected.to_bits(), "parameter {i}");
        }
        let targets = [GroupWeight {
            group: 0,
            weight: 1,
        }];
        let other = [GroupWeight {
            group: 1,
            weight: 1,
        }];
        let anchors: Vec<_> = (0..17)
            .map(|i| TrainingSample {
                query: if i % 2 == 0 { &[1.] } else { &[-1.] },
                neighbors: if i < 16 { &targets } else { &other },
                requested_rows: 1,
            })
            .collect();
        let mut actual = initialized.clone();
        let mut expected = initialized.clone();
        let observed_model = Model::new(1, 2, &initialized).unwrap();
        let observed_digests = [[1; 32], [0; 32], [0; 32], [2; 32]];
        let observed_membership =
            Membership::new(1, 2, 32, &[0, 3], &observed_digests, limits()).unwrap();
        let normalized =
            selector::prepare_training(&observed_model, &observed_membership, &anchors, limits())
                .unwrap();
        // Independent scalar gradient for each anchor, accumulated in the
        // literal 16+1 batches and stored f32 after every SGD step.
        for _ in 0..16 {
            for (batch, samples) in anchors.chunks(16).enumerate() {
                let mut grad = vec![0f64; expected.len()];
                for (i, sample) in samples.iter().enumerate() {
                    let target = if batch * 16 + i < 16 { 0 } else { 3 };
                    let independent = scalar_oracle(&expected, sample.query, 2, target).1;
                    for (g, x) in grad.iter_mut().zip(independent) {
                        *g += x;
                    }
                }
                for (p, g) in expected.iter_mut().zip(grad) {
                    *p = (f64::from(*p) - g / (256. * samples.len() as f64)) as f32;
                }
            }
        }
        train(
            input(32, &anchors),
            2,
            &mut actual,
            &[0, 3],
            &normalized,
            1.,
            0,
            &mut a,
            &mut |_| true,
        )
        .unwrap();
        // Independent Jacobian expressions have different f64 association;
        // compare stored values within one f32 rounding unit.
        for (x, y) in actual.iter().zip(expected) {
            assert!((x - y).abs() < 2e-8, "{x} != {y}");
        }
    }
    #[test]
    fn oracle_fitter_tiny_actual_pipeline_determinism_acceptance_and_rollback() {
        // 57 groups force side2 and two occupied labels. The initially
        // missed teacher must become covered by one-body serving.
        let initial = independent_initial(1, 2);
        let probabilities = scalar_oracle(&initial, &[1.], 2, 0).0;
        let mut score = [0f32; 2];
        for label in 0..2 {
            for r in 0..4 {
                score[label] += (probabilities[r] * probabilities[4 + r * 2])
                    * probabilities[12 + r * 2 + label];
            }
        }
        // Choose the initially missed occupied label using independent scalar
        // arithmetic, so the tiny fit must exhibit a real accepted gain.
        let teacher = if score[0] >= score[1] { 56 } else { 0 };
        let neighbors = [GroupWeight {
            group: teacher,
            weight: 1,
        }];
        let anchors = [TrainingSample {
            query: &[1.],
            neighbors: &neighbors,
            requested_rows: 1,
        }];
        let source = input(897, &anchors); // one-row short final group
        let a = tiny(source, limits(), |_| true).unwrap();
        let b = tiny(source, limits(), |_| true).unwrap();
        assert_eq!(a.parameters, b.parameters);
        assert_eq!(a.owners, b.owners);
        assert_eq!(a.diagnostic_tokens, b.diagnostic_tokens);
        assert_eq!(a.events, b.events);
        assert_eq!(a.model, b.model);
        assert_eq!(a.membership, b.membership);
        assert_eq!(a.snapshot, source.snapshot);
        assert_eq!(a.budget, source.budget);
        assert_eq!(a.work, b.work);
        assert_eq!(a.memory, b.memory);
        assert_eq!(
            a.events
                .iter()
                .filter(|e| matches!(e, FitEvent::Checkpoint { .. }))
                .count(),
            2
        );
        assert_eq!(
            a.events
                .iter()
                .filter(|e| matches!(e, FitEvent::NoTeacher { .. }))
                .count(),
            112
        );
        let mut accepted = false;
        let mut refused = false;
        for event in &a.events {
            if let FitEvent::Checkpoint { receipt, .. } | FitEvent::Proposal { receipt, .. } = event
            {
                if receipt.accepted() {
                    assert!(receipt.after_total > receipt.before_total);
                    assert!(receipt.after_lower_tail >= receipt.before_lower_tail);
                    accepted = true;
                } else {
                    refused = true;
                }
            }
        }
        assert!(accepted && refused);
        let model = Model::new(1, a.side, &a.parameters).unwrap();
        let member =
            Membership::new(1, a.side, 897, &a.owners, &a.diagnostic_tokens, limits()).unwrap();
        let final_plan = selector::select(
            &model,
            &member,
            &[1.],
            &neighbors,
            source.snapshot,
            source.budget,
            1,
            limits(),
        )
        .unwrap();
        assert_eq!(final_plan.covered_weight(), 1);
        // Identical queries with different teacher objects cannot improve by
        // fitting a one-body selector. A move must unite the teachers instead.
        let left = [GroupWeight {
            group: 0,
            weight: 1,
        }];
        let right = [GroupWeight {
            group: 56,
            weight: 1,
        }];
        let pair = [
            TrainingSample {
                query: &[1.],
                neighbors: &left,
                requested_rows: 1,
            },
            TrainingSample {
                query: &[1.],
                neighbors: &right,
                requested_rows: 1,
            },
        ];
        let moved = tiny(input(897, &pair), limits(), |_| true).unwrap();
        assert_eq!(moved.parameters, independent_initial(1, 2));
        assert_eq!(moved.owners[0], moved.owners[56]);
        assert!(moved.events.iter().any(|e|matches!(e,FitEvent::Proposal {receipt,..} if receipt.accepted() && receipt.before_total==1 && receipt.after_total==2)));
        // Every gain-free checkpoint must restore the preceding parameters.
        // With one object, coverage cannot improve from the initial model.
        let small_neighbors = [GroupWeight {
            group: 0,
            weight: 1,
        }];
        let small = [TrainingSample {
            query: &[1.],
            neighbors: &small_neighbors,
            requested_rows: 1,
        }];
        let rollback = tiny(input(1, &small), limits(), |_| true).unwrap();
        let original = independent_initial(1, 1);
        assert_eq!(rollback.parameters, original);
        assert!(rollback.events.iter().all(|e|matches!(e,FitEvent::Checkpoint {receipt,..} if receipt.decision==AcceptanceDecision::Refused(Refusal::ZeroGain))));
    }
    #[test]
    fn oracle_fitter_actual_lower_tail_checkpoint_rollback_and_current_state() {
        let initial = independent_initial(1, 2);
        let probabilities = scalar_oracle(&initial, &[1.], 2, 0).0;
        let mut scores = [0f32; 4];
        for (label, score) in scores.iter_mut().enumerate() {
            for r in 0..4 {
                *score += (probabilities[r] * probabilities[4 + r * 2 + label / 2])
                    * probabilities[12 + r * 2 + label % 2];
            }
        }
        let mut occupied = [0usize, 1, 2];
        occupied.sort_by(|&a, &b| scores[b].total_cmp(&scores[a]).then(a.cmp(&b)));
        let a = occupied[0];
        let b = occupied[1];
        let c = occupied[2];
        let low = [
            GroupWeight {
                group: (a * 56) as u32,
                weight: 1,
            },
            GroupWeight {
                group: (a * 56 + 1) as u32,
                weight: 1,
            },
            GroupWeight {
                group: (c * 56) as u32,
                weight: 8,
            },
        ];
        let high = [
            GroupWeight {
                group: (a * 56) as u32,
                weight: 2,
            },
            GroupWeight {
                group: (b * 56) as u32,
                weight: 8,
            },
        ];
        let samples: Vec<_> = (0..256)
            .map(|i| TrainingSample {
                query: &[1.],
                neighbors: if i < 20 { &low } else { &high },
                requested_rows: 1,
            })
            .collect();
        // Fixture mass10 uses the real fixed optimizer and two alternating
        // rounds. One body cannot cover both minority and majority objects.
        let result = fit_with_mass(input(2688, &samples), limits(), |_| true, 10.).unwrap();
        let FitEvent::Checkpoint { round: 0, receipt } = &result.events[0] else {
            panic!("first checkpoint missing")
        };
        assert_eq!(
            (
                receipt.before_total,
                receipt.after_total,
                receipt.before_lower_tail,
                receipt.after_lower_tail,
                receipt.lower_tail_rank
            ),
            (512, 1888, 2, 0, 13)
        );
        assert_eq!(
            receipt.decision,
            AcceptanceDecision::Refused(Refusal::LowerTailRegression)
        );
        assert_ne!(receipt.before_model, receipt.after_model);
        let original_model = receipt.before_model;
        let mut current_membership = receipt.before_membership;
        let mut accepted = 0;
        for event in &result.events {
            if let FitEvent::Proposal {
                round: 0, receipt, ..
            } = event
            {
                // Checkpoint rollback restores the original model/cache, and
                // each sequential proposal binds the last accepted membership.
                assert_eq!(receipt.before_model, original_model);
                assert_eq!(receipt.after_model, original_model);
                assert_eq!(receipt.before_membership, current_membership);
                if receipt.accepted() {
                    current_membership = receipt.after_membership;
                    accepted += 1;
                }
            }
        }
        assert!(accepted >= 2);
    }
    #[test]
    fn oracle_fitter_capacity_move_smallest_swap_and_production_mass() {
        let mut owners = vec![0u32; 64];
        owners.push(1);
        let mut digests = vec![[0; 32]; 4];
        digests[0] = [1; 32];
        digests[1] = [2; 32];
        let old = Membership::new(1, 2, 1025, &owners, &digests, limits()).unwrap();
        let mut output = vec![9u32; 65];
        let mut ledger = Admission::new(limits());
        assert_eq!(
            propose(&old, 64, 0, &mut output, &mut ledger).unwrap(),
            MoveKind::Swap(0)
        );
        let mut expected = vec![0u32; 65];
        expected[0] = 1;
        assert_eq!(output, expected);
        let swapped = Membership::new(1, 2, 1025, &output, &digests, limits()).unwrap();
        assert_eq!(
            (swapped.objects()[0].groups, swapped.objects()[0].rows),
            (64, 1009)
        );
        assert_eq!(
            (swapped.objects()[1].groups, swapped.objects()[1].rows),
            (1, 16)
        );
        assert_eq!(
            propose(&old, 0, 1, &mut output, &mut ledger).unwrap(),
            MoveKind::Move
        );
        assert_eq!(output[0], 1);
        assert_eq!(output[64], 1);
        assert!(propose(&old, 65, 0, &mut output, &mut ledger).is_err());
        assert!(propose(&old, 0, 4, &mut output, &mut ledger).is_err());
        let untouched = output.clone();
        assert!(
            propose(
                &old,
                64,
                0,
                &mut output,
                &mut Admission::new(Limits {
                    operations: 1,
                    ..limits()
                })
            )
            .is_err()
        );
        assert_eq!(output, untouched);
        let weights: Vec<_> = (0..7)
            .map(|g| GroupWeight {
                group: g,
                weight: if g == 6 { 4 } else { 16 },
            })
            .collect();
        let anchors = [TrainingSample {
            query: &[2.],
            neighbors: &weights,
            requested_rows: 100,
        }];
        let result = fit(input(113, &anchors), limits(), |_| true).unwrap();
        assert_eq!(result.side, 1);
        assert_eq!(result.parameters, independent_initial(1, 1));
        assert_eq!(result.owners, vec![0; 8]);
        let source = input(113, &anchors);
        let stopped = fit(source, limits(), |p| p.operations < 100).unwrap_err();
        assert_eq!(
            stopped.error,
            Error::Refused("caller stopped incomplete fitting")
        );
        assert!(stopped.work.operations > 100);
        assert!(stopped.memory.peak_capacity_bytes > 0);
        assert_eq!(stopped.memory.retained_capacity_bytes, 0);
        assert_eq!(anchors[0].query, &[2.]);
        assert_eq!(
            weights.iter().map(|w| u64::from(w.weight)).sum::<u64>(),
            100
        );
        for event in &result.events {
            if let FitEvent::Checkpoint { receipt, .. } = event {
                assert_eq!((receipt.before_total, receipt.after_total), (100, 100));
                assert_eq!(
                    receipt.decision,
                    AcceptanceDecision::Refused(Refusal::ZeroGain)
                );
            }
        }
    }

    #[test]
    fn oracle_fitter_weighted_destinations_anchor_order_and_label_ties() {
        let mut parameters = vec![0f32; 64];
        parameters[0] = 1.;
        let mut biases = vec![0f32; 64];
        biases[0] = 1.;
        parameters.extend(biases);
        parameters.extend(vec![0.; 4 * 64]);
        parameters.extend([0.; 4]);
        for _ in 0..4 {
            let mut first = vec![0f32; 64];
            first[0] = 2.;
            parameters.extend(first);
            let mut second = vec![0f32; 64];
            second[0] = -2.;
            parameters.extend(second);
        }
        parameters.extend([-2., 2., -2., 2., -2., 2., -2., 2.]);
        parameters.extend(vec![0.; 4 * 2 * 64]);
        parameters.extend([0.; 8]);
        let model = Model::new(1, 2, &parameters).unwrap();
        let member = Membership::new(
            1,
            2,
            32,
            &[0, 1],
            &[[1; 32], [2; 32], [0; 32], [0; 32]],
            limits(),
        )
        .unwrap();
        let one = [GroupWeight {
            group: 0,
            weight: 1,
        }];
        let sixteen = [GroupWeight {
            group: 0,
            weight: 16,
        }];
        let excluded = [GroupWeight {
            group: 1,
            weight: 16,
        }];
        let anchors = [
            TrainingSample {
                query: &[1.],
                neighbors: &one,
                requested_rows: 1,
            },
            TrainingSample {
                query: &[1.],
                neighbors: &one,
                requested_rows: 1,
            },
            TrainingSample {
                query: &[-1.],
                neighbors: &sixteen,
                requested_rows: 1,
            },
            TrainingSample {
                query: &[1.],
                neighbors: &excluded,
                requested_rows: 1,
            },
        ];
        let prepared = selector::prepare_training(&model, &member, &anchors, limits()).unwrap();
        let mut a = Admission::new(limits());
        let cache = caches(&model, &prepared, &mut a).unwrap();
        let mut aggregate = vec![0f64; 4];
        let actual = destinations(input(32, &anchors), &cache, 0, 0, &mut aggregate, &mut a)
            .unwrap()
            .unwrap();
        let mut expected = [0f64; 4];
        for (anchor, count) in [(0, 1.), (1, 1.), (2, 16.)] {
            let probabilities = scalar_oracle(&parameters, anchors[anchor].query, 2, 0).0;
            for label in 0..4 {
                let mut score = 0f32;
                for r in 0..4 {
                    score += (probabilities[r] * probabilities[4 + r * 2 + label / 2])
                        * probabilities[12 + r * 2 + label % 2];
                }
                expected[label] += count * f64::from(score);
            }
        }
        assert_eq!(aggregate, expected);
        // Teacher count16 outweighs the two positive anchors. Identical
        // second heads give bit-identical column ties, resolved by label.
        assert_eq!(&actual[..3], &[2, 3, 1]);
        assert_eq!(actual[3], u32::MAX);
        assert_eq!(expected[0], expected[1]);
        assert_eq!(expected[2], expected[3]);
        assert!(
            destinations(input(32, &anchors), &cache, 2, 0, &mut aggregate, &mut a)
                .unwrap()
                .is_none()
        );
        // Destinations use every label, even outside the serving Cartesian set.
        // Uniform side10 serves columns 0..7; owner0 must not consume a slot.
        let parameters = vec![0f32; 64 * 2 + 4 * 65 + 8 * 10 * 65];
        let model = Model::new(1, 10, &parameters).unwrap();
        let mut tokens = vec![[0; 32]; 100];
        tokens[0] = [1; 32];
        let member = Membership::new(1, 10, 16, &[0], &tokens, limits()).unwrap();
        let samples = [TrainingSample {
            query: &[1.],
            neighbors: &one,
            requested_rows: 1,
        }];
        let training = selector::prepare_training(&model, &member, &samples, limits()).unwrap();
        let cache = caches(&model, &training, &mut a).unwrap();
        assert!(!cache[0].candidates().contains(&8));
        assert_eq!(
            destinations(
                input(16, &samples),
                &cache,
                0,
                0,
                &mut vec![0.; 100],
                &mut a
            )
            .unwrap()
            .unwrap(),
            [1, 2, 3, 4, 5, 6, 7, 8]
        );
    }
    #[test]
    fn oracle_fitter_diagnostic_tokens_bind_source_and_complete_membership() {
        let source = input(32, &[]);
        let mut a = Admission::new(limits());
        let mut initial = [[0; 32]; 4];
        diagnostic_tokens(source, 2, &[0, 1], &mut initial, &mut a).unwrap();
        let mut swapped = [[0; 32]; 4];
        diagnostic_tokens(source, 2, &[1, 0], &mut swapped, &mut a).unwrap();
        assert_ne!(initial[0], swapped[0]);
        assert_ne!(initial[1], swapped[1]);
        assert_eq!(&initial[2..], &[[0; 32]; 2]);
        assert_eq!(&initial[0][..4], &1u32.to_le_bytes());
        assert_eq!(&initial[1][..4], &2u32.to_le_bytes());
        // Independent canonical byte encoding, not a production hash helper.
        let mut bytes = b"BORSUK-budget-fitter-layout-v1".to_vec();
        bytes.extend([7; 32]);
        bytes.extend(1u32.to_le_bytes());
        bytes.extend(2u32.to_le_bytes());
        bytes.extend(32u64.to_le_bytes());
        bytes.extend(2u64.to_le_bytes());
        bytes.extend(0u32.to_le_bytes());
        bytes.extend(1u32.to_le_bytes());
        let layout = Sha256::digest(&bytes);
        let mut bytes = b"BORSUK-budget-fitter-offline-body-v1".to_vec();
        bytes.extend([7; 32]);
        bytes.extend(layout);
        bytes.extend(0u32.to_le_bytes());
        let mut expected: Hash = Sha256::digest(&bytes).into();
        expected[..4].copy_from_slice(&1u32.to_le_bytes());
        assert_eq!(initial[0], expected);
        for changed in [
            FitInput {
                source: [8; 32],
                ..source
            },
            FitInput { rows: 31, ..source },
        ] {
            let mut other = [[0; 32]; 4];
            diagnostic_tokens(changed, 2, &[0, 1], &mut other, &mut a).unwrap();
            assert_ne!(initial, other);
        }
    }
    #[test]
    fn oracle_fitter_refusal_caps_coexistence_corruption_and_source_unchanged() {
        let mut a = Admission::new(Limits {
            operations: 50,
            ..limits()
        });
        let failed: Result<(), Error> = run_selector(&mut a, |child| {
            child.charge(17)?;
            let _scratch = child.vector::<u64>(32)?;
            child.charge(34)
        });
        assert_eq!(failed, Err(Error::Refused("compute ceiling")));
        assert_eq!(a.work, 17);
        assert_eq!(a.peak, 32 * size_of::<u64>());
        assert_eq!(a.bytes, 0);
        let neighbors = [GroupWeight {
            group: 0,
            weight: 1,
        }];
        let anchors = [TrainingSample {
            query: &[1.],
            neighbors: &neighbors,
            requested_rows: 1,
        }];
        let source = input(1, &anchors);
        let mut reported = 0;
        let stopped = tiny(source, limits(), |p| {
            reported = p.operations;
            false
        })
        .unwrap_err();
        assert_eq!(reported, stopped.work.operations);
        assert_eq!(stopped.stage, FitStage::Admission);
        assert!(!stopped.completed);
        assert_eq!(stopped.accepted, None);
        assert!(reported > 0);
        // Cancellation after candidate inference/evaluation, before acceptance,
        // keeps the declared state identities and all consumed work/capacity.
        let left = [GroupWeight {
            group: 0,
            weight: 1,
        }];
        let right = [GroupWeight {
            group: 56,
            weight: 1,
        }];
        let pair = [
            TrainingSample {
                query: &[1.],
                neighbors: &left,
                requested_rows: 1,
            },
            TrainingSample {
                query: &[1.],
                neighbors: &right,
                requested_rows: 1,
            },
        ];
        let mut constructed = 0;
        let mut evaluated = 0;
        let mut finishes = 0;
        let rejected = tiny(input(897, &pair), limits(), |p| {
            if p.group.is_none() && p.epoch == 16 {
                finishes += 1;
                if finishes == 1 {
                    constructed = p.operations;
                }
                if finishes == 2 {
                    evaluated = p.operations;
                    return false;
                }
            }
            true
        })
        .unwrap_err();
        assert_eq!(finishes, 2);
        assert_eq!(
            rejected.error,
            Error::Refused("caller stopped incomplete fitting")
        );
        assert_eq!(rejected.stage, FitStage::Checkpoint { round: 0 });
        assert!(!rejected.completed);
        assert!(evaluated > constructed);
        assert_eq!(rejected.work.operations, evaluated);
        assert_eq!(rejected.memory.retained_capacity_bytes, 0);
        assert!(rejected.memory.peak_capacity_bytes > 2 * (1428 * 4));
        let restored = rejected.accepted.unwrap();
        let mut model_bytes = b"BORSUK-budget-model-v1".to_vec();
        model_bytes.extend(1u32.to_le_bytes());
        model_bytes.extend(2u32.to_le_bytes());
        for value in independent_initial(1, 2) {
            model_bytes.extend(value.to_bits().to_le_bytes());
        }
        let original_model: Hash = Sha256::digest(&model_bytes).into();
        assert_eq!(restored.model, original_model);
        assert_eq!(restored.source, [7; 32]);
        assert_ne!(restored.membership, [0; 32]);
        assert_ne!(restored.training, [0; 32]);
        assert_eq!(restored.snapshot, source.snapshot);
        assert_eq!(restored.budget, source.budget);
        assert!(
            tiny(
                source,
                Limits {
                    transient_bytes: 1,
                    ..limits()
                },
                |_| true
            )
            .is_err()
        );
        assert!(
            tiny(
                source,
                Limits {
                    operations: 1,
                    ..limits()
                },
                |_| true
            )
            .is_err()
        );
        assert!(
            tiny(
                source,
                Limits {
                    operations: MAX_OPERATIONS + 1,
                    ..limits()
                },
                |_| true
            )
            .is_err()
        );
        assert!(tiny(source, limits(), |_| false).is_err());
        let mut batches = 0;
        assert!(
            tiny(source, limits(), |p| {
                if p.group.is_none() {
                    batches += 1;
                }
                batches < 3
            })
            .is_err()
        );
        // A limit below the measured coexistence refuses before a result exists.
        let result = tiny(source, limits(), |_| true).unwrap();
        let exhausted = tiny(
            source,
            Limits {
                operations: result.work.operations - 1,
                ..limits()
            },
            |_| true,
        )
        .unwrap_err();
        assert!(exhausted.work.operations > result.work.operations / 2);
        assert!(exhausted.work.operations <= result.work.operations - 1);
        assert_eq!(exhausted.stage, FitStage::Finalization);
        assert!(!exhausted.completed);
        assert_eq!(exhausted.memory.retained_capacity_bytes, 0);
        let restored = exhausted.accepted.unwrap();
        assert_eq!(restored.model, result.model);
        assert_eq!(restored.membership, result.membership);
        assert_eq!(restored.source, source.source);
        assert_eq!(restored.training, result.training);
        assert_eq!(restored.snapshot, source.snapshot);
        assert_eq!(restored.budget, source.budget);
        assert!(
            tiny(
                source,
                Limits {
                    transient_bytes: result.memory.peak_capacity_bytes - 1,
                    ..limits()
                },
                |_| true
            )
            .is_err()
        );
        assert_eq!(anchors[0].query, &[1.]);
        assert_eq!(
            neighbors,
            [GroupWeight {
                group: 0,
                weight: 1
            }]
        );
        assert!(fit(source, limits(), |_| true).is_err()); // production requires mass100
        assert!(tiny(FitInput { rows: 0, ..source }, limits(), |_| true).is_err());
        assert!(
            tiny(
                FitInput {
                    source: [0; 32],
                    ..source
                },
                limits(),
                |_| true
            )
            .is_err()
        );
        assert!(
            tiny(
                FitInput {
                    anchors: &[],
                    ..source
                },
                limits(),
                |_| true
            )
            .is_err()
        );
        for query in [[0.], [f32::NAN], [f32::INFINITY]] {
            let invalid = [TrainingSample {
                query: &query,
                ..anchors[0]
            }];
            assert!(
                tiny(
                    FitInput {
                        anchors: &invalid,
                        ..source
                    },
                    limits(),
                    |_| true
                )
                .is_err()
            );
        }
        let bad = [GroupWeight {
            group: 0,
            weight: 2,
        }];
        let invalid = [TrainingSample {
            neighbors: &bad,
            ..anchors[0]
        }];
        assert!(
            tiny(
                FitInput {
                    anchors: &invalid,
                    ..source
                },
                limits(),
                |_| true
            )
            .is_err()
        );
        let duplicate = [
            GroupWeight {
                group: 0,
                weight: 1,
            },
            GroupWeight {
                group: 0,
                weight: 1,
            },
        ];
        let invalid = [TrainingSample {
            neighbors: &duplicate,
            ..anchors[0]
        }];
        assert!(
            tiny(
                FitInput {
                    anchors: &invalid,
                    ..source
                },
                limits(),
                |_| true
            )
            .is_err()
        );
        let underfill = [TrainingSample {
            requested_rows: 100,
            ..anchors[0]
        }];
        let mut callbacks = Vec::new();
        let result = tiny(
            FitInput {
                anchors: &underfill,
                ..source
            },
            limits(),
            |p| {
                callbacks.push(p);
                true
            },
        )
        .unwrap_err();
        assert_eq!(
            result.error,
            Error::Refused("initial incomplete serving coverage")
        );
        assert_eq!(result.stage, FitStage::InitialCoverage);
        assert!(!result.completed);
        assert_eq!(result.memory.retained_capacity_bytes, 0);
        assert_eq!(callbacks.len(), 1); // no epoch/batch callback
        assert_eq!(
            (
                callbacks[0].round,
                callbacks[0].epoch,
                callbacks[0].batch,
                callbacks[0].group
            ),
            (0, 0, 0, None)
        );
        assert!(result.work.operations > callbacks[0].operations);
        assert!(
            result.memory.peak_capacity_bytes
                > size_of::<TrainingSample<'_>>() + size_of::<f32>() + size_of::<GroupWeight>()
        );
        assert!(result.accepted.is_some());
        // The same pre-epoch refusal through the production mass100 entrypoint.
        let weights: Vec<_> = (0..7)
            .map(|group| GroupWeight {
                group,
                weight: if group == 6 { 4 } else { 16 },
            })
            .collect();
        let samples = [TrainingSample {
            query: &[1.],
            neighbors: &weights,
            requested_rows: 114,
        }];
        let mut calls = 0;
        let failure = fit(input(113, &samples), limits(), |_| {
            calls += 1;
            true
        })
        .unwrap_err();
        assert_eq!(calls, 1);
        assert_eq!(
            failure.error,
            Error::Refused("initial incomplete serving coverage")
        );
        assert_eq!(failure.stage, FitStage::InitialCoverage);
        assert!(!failure.completed);
        assert!(failure.work.operations > 80);
        assert_eq!(failure.memory.retained_capacity_bytes, 0);
    }
}
