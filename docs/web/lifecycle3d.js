// End-to-end 3D wizard of a BORSUK index's whole life, rendered with three.js.
// Start empty, add vectors, watch them bubble into segments, oversized bubbles
// split, deletes tombstone rows, and compaction drops the dead rows and merges
// sparse bubbles. It's a live simulation: the clustering, splitting, and merging
// all run in JS and drive tweened meshes — no canned animation.
//
// Loaded as its own ES module (kept out of app.js so the fake-DOM docs test is
// unaffected). If three.js can't be fetched, the fallback text stays put.

const THREE_URL = "https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.module.js";

const CAPACITY = 8; // a segment splits once it holds more than this many rows
const MIN_LIVE = 2; // a segment merges once its live rows fall to/under this
const NEW_SEG_DIST = 2.6; // farther than this from every centroid ⇒ new segment
const PALETTE = [
  0x2f7f73, 0xc14d32, 0x6f4a31, 0x3f6b57, 0xb07a3f, 0x9d3b26, 0x4f7d8c, 0x7d6b53,
];
const ATTRACTORS = [
  [-3.2, 1.1, 0.2],
  [3.0, 1.6, -1.1],
  [0.2, -2.4, 2.0],
  [2.2, -1.0, -2.6],
  [-2.4, -1.6, -1.4],
  [-0.6, 2.6, -1.8],
];

const dist = (a, b) => Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]);
const jitter = () => (Math.random() - 0.5) * 1.7;

export async function initLifecycle3d() {
  const mount = document.querySelector("[data-lifecycle3d]");
  if (!mount) return;

  let THREE;
  try {
    THREE = await import(/* @vite-ignore */ THREE_URL);
  } catch {
    mount.innerHTML =
      '<p class="viz-fallback">The interactive build needs a network connection to load three.js. In short: add vectors → they group into segment bubbles → oversized bubbles split → deletes tombstone rows → compaction drops the dead rows and merges sparse bubbles.</p>';
    return;
  }

  // --- Simulation state ---------------------------------------------------
  const sim = { vectors: [], segments: [], nextVec: 0, nextSeg: 0, nextColor: 0 };

  const nextColor = () => PALETTE[sim.nextColor++ % PALETTE.length];

  const recompute = (seg) => {
    const live = seg.members.filter((m) => m.alive);
    const pts = live.length ? live : seg.members;
    if (!pts.length) return;
    seg.centroid = [0, 1, 2].map((a) => pts.reduce((t, m) => t + m.pos[a], 0) / pts.length);
    seg.radius = Math.max(0.55, ...pts.map((m) => dist(m.pos, seg.centroid) + 0.28));
  };

  const assign = (v) => {
    let best = null;
    let bd = Infinity;
    for (const s of sim.segments) {
      const d = dist(v.pos, s.centroid);
      if (d < bd) {
        bd = d;
        best = s;
      }
    }
    if (!best || bd > NEW_SEG_DIST) {
      best = { id: sim.nextSeg++, color: nextColor(), centroid: v.pos.slice(), radius: 0.55, members: [] };
      sim.segments.push(best);
    }
    best.members.push(v);
    v.seg = best;
    recompute(best);
  };

  const addVectors = (n) => {
    for (let i = 0; i < n; i += 1) {
      const a = ATTRACTORS[Math.floor(Math.random() * ATTRACTORS.length)];
      const v = {
        id: sim.nextVec++,
        pos: [a[0] + jitter(), a[1] + jitter(), a[2] + jitter()],
        alive: true,
        seg: null,
        spawn: null,
      };
      assign(v);
      sim.vectors.push(v);
    }
  };

  const twoMeans = (members) => {
    // Seed with the two farthest-apart members, then a few Lloyd iterations.
    let s0 = members[0];
    let s1 = members[1] || members[0];
    let far = -1;
    for (const a of members)
      for (const b of members) {
        const d = dist(a.pos, b.pos);
        if (d > far) {
          far = d;
          s0 = a;
          s1 = b;
        }
      }
    let c0 = s0.pos.slice();
    let c1 = s1.pos.slice();
    let ga = [];
    let gb = [];
    for (let iter = 0; iter < 4; iter += 1) {
      ga = [];
      gb = [];
      for (const m of members) (dist(m.pos, c0) <= dist(m.pos, c1) ? ga : gb).push(m);
      if (!ga.length || !gb.length) break;
      const mean = (g, ax) => g.reduce((t, m) => t + m.pos[ax], 0) / g.length;
      c0 = [0, 1, 2].map((ax) => mean(ga, ax));
      c1 = [0, 1, 2].map((ax) => mean(gb, ax));
    }
    return [ga, gb];
  };

  const splitOversized = () => {
    let did = 0;
    for (const seg of [...sim.segments]) {
      if (seg.members.filter((m) => m.alive).length <= CAPACITY) continue;
      const [ga, gb] = twoMeans(seg.members);
      if (!ga.length || !gb.length) continue;
      seg.members = ga;
      ga.forEach((m) => (m.seg = seg));
      recompute(seg);
      const other = { id: sim.nextSeg++, color: nextColor(), centroid: [0, 0, 0], radius: 0.55, members: gb };
      gb.forEach((m) => (m.seg = other));
      recompute(other);
      sim.segments.push(other);
      did += 1;
    }
    return did;
  };

  const deleteSome = (n) => {
    const alive = sim.vectors.filter((v) => v.alive);
    let removed = 0;
    for (let i = 0; i < n && alive.length; i += 1) {
      const idx = Math.floor(Math.random() * alive.length);
      alive[idx].alive = false;
      alive.splice(idx, 1);
      removed += 1;
    }
    sim.segments.forEach(recompute);
    return removed;
  };

  const compact = () => {
    // Physically drop tombstoned rows.
    sim.vectors = sim.vectors.filter((v) => v.alive);
    for (const s of sim.segments) s.members = s.members.filter((m) => m.alive);
    sim.segments = sim.segments.filter((s) => s.members.length);
    // Merge sparse bubbles into their nearest neighbour.
    let changed = true;
    while (changed && sim.segments.length > 1) {
      changed = false;
      for (const s of sim.segments) {
        if (s._gone || s.members.length > MIN_LIVE) continue;
        let best = null;
        let bd = Infinity;
        for (const o of sim.segments) {
          if (o === s || o._gone) continue;
          const d = dist(s.centroid, o.centroid);
          if (d < bd) {
            bd = d;
            best = o;
          }
        }
        if (!best) continue;
        for (const m of s.members) {
          m.seg = best;
          best.members.push(m);
        }
        s._gone = true;
        recompute(best);
        changed = true;
      }
      sim.segments = sim.segments.filter((s) => !s._gone);
    }
    sim.segments.forEach(recompute);
  };

  // --- three.js scene -----------------------------------------------------
  const scene = new THREE.Scene();
  const world = new THREE.Group();
  scene.add(world);
  scene.add(new THREE.HemisphereLight(0xffffff, 0xccd0c2, 1.15));
  const key = new THREE.DirectionalLight(0xffffff, 1.35);
  key.position.set(5, 7, 6);
  scene.add(key);
  const warm = new THREE.DirectionalLight(0xffd9b0, 0.5);
  warm.position.set(-6, -3, -3);
  scene.add(warm);

  const pointGeo = new THREE.SphereGeometry(0.14, 20, 20);
  const coreGeo = new THREE.SphereGeometry(0.2, 18, 18);
  const shellGeo = new THREE.SphereGeometry(1, 32, 32);
  const wireGeo = new THREE.WireframeGeometry(new THREE.SphereGeometry(1, 14, 10));
  const pointMap = new Map(); // vector id -> mesh
  const bubbleMap = new Map(); // segment id -> group
  const coreMap = new Map(); // segment id -> centroid marker mesh

  const col = (hex) => new THREE.Color(hex);

  const syncScene = () => {
    const seenV = new Set();
    for (const v of sim.vectors) {
      seenV.add(v.id);
      let m = pointMap.get(v.id);
      if (!m) {
        m = new THREE.Mesh(
          pointGeo,
          new THREE.MeshStandardMaterial({ color: v.seg.color, roughness: 0.32, metalness: 0.1, transparent: true }),
        );
        // Fly in from a random point on a big sphere.
        const s = 9;
        m.position.set((Math.random() - 0.5) * s, (Math.random() - 0.5) * s, (Math.random() - 0.5) * s);
        m.scale.setScalar(0.01);
        world.add(m);
        pointMap.set(v.id, m);
      }
      m.userData.t = {
        pos: v.pos,
        op: v.alive ? 1 : 0.16,
        scale: v.alive ? 1 : 0.55,
        color: v.seg.color,
      };
    }
    for (const [id, m] of pointMap) {
      if (!seenV.has(id)) m.userData.t = { pos: m.position.toArray(), op: 0, scale: 0.01, gone: true };
    }

    const seenS = new Set();
    for (const s of sim.segments) {
      seenS.add(s.id);
      let g = bubbleMap.get(s.id);
      if (!g) {
        const shell = new THREE.Mesh(
          shellGeo,
          new THREE.MeshStandardMaterial({
            color: s.color,
            transparent: true,
            opacity: 0.0,
            roughness: 0.7,
            side: THREE.DoubleSide,
            depthWrite: false,
          }),
        );
        const wire = new THREE.LineSegments(
          wireGeo,
          new THREE.LineBasicMaterial({ color: s.color, transparent: true, opacity: 0.0 }),
        );
        g = new THREE.Group();
        g.add(shell, wire);
        g.userData.shell = shell;
        g.userData.wire = wire;
        g.position.set(s.centroid[0], s.centroid[1], s.centroid[2]);
        g.scale.setScalar(s.radius);
        world.add(g);
        bubbleMap.set(s.id, g);
      }
      g.userData.t = { pos: s.centroid, radius: s.radius, op: 1, color: s.color };

      // A dark centroid marker: the running mean of the segment's vectors. It
      // visibly drifts to the new average each time a vector joins.
      let core = coreMap.get(s.id);
      if (!core) {
        core = new THREE.Mesh(
          coreGeo,
          new THREE.MeshStandardMaterial({ color: 0x26352d, roughness: 0.4, transparent: true, opacity: 0 }),
        );
        core.position.set(s.centroid[0], s.centroid[1], s.centroid[2]);
        world.add(core);
        coreMap.set(s.id, core);
      }
      core.userData.t = { pos: s.centroid, op: 0.92 };
    }
    for (const [id, g] of bubbleMap) {
      if (!seenS.has(id)) g.userData.t = { pos: g.position.toArray(), radius: g.scale.x, op: 0, gone: true };
    }
    for (const [id, core] of coreMap) {
      if (!seenS.has(id)) core.userData.t = { pos: core.position.toArray(), op: 0, gone: true };
    }
    updateStatus();
  };

  // --- Tween loop ---------------------------------------------------------
  const lerp = (a, b, t) => a + (b - a) * t;
  const step = () => {
    const k = 0.14;
    for (const [id, m] of pointMap) {
      const t = m.userData.t;
      if (!t) continue;
      m.position.x = lerp(m.position.x, t.pos[0], k);
      m.position.y = lerp(m.position.y, t.pos[1], k);
      m.position.z = lerp(m.position.z, t.pos[2], k);
      const sc = lerp(m.scale.x, t.scale, k);
      m.scale.setScalar(sc);
      m.material.opacity = lerp(m.material.opacity, t.op, k);
      m.material.color.lerp(col(t.color), k);
      if (t.gone && m.material.opacity < 0.03) {
        world.remove(m);
        pointMap.delete(id);
      }
    }
    for (const [id, g] of bubbleMap) {
      const t = g.userData.t;
      if (!t) continue;
      g.position.x = lerp(g.position.x, t.pos[0], k);
      g.position.y = lerp(g.position.y, t.pos[1], k);
      g.position.z = lerp(g.position.z, t.pos[2], k);
      g.scale.setScalar(lerp(g.scale.x, t.radius, k));
      const shell = g.userData.shell;
      const wire = g.userData.wire;
      shell.material.opacity = lerp(shell.material.opacity, t.op * 0.1, k);
      wire.material.opacity = lerp(wire.material.opacity, t.op * 0.26, k);
      shell.material.color.lerp(col(t.color), k);
      wire.material.color.lerp(col(t.color), k);
      if (t.gone && wire.material.opacity < 0.01) {
        world.remove(g);
        bubbleMap.delete(id);
      }
    }
    for (const [id, core] of coreMap) {
      const t = core.userData.t;
      if (!t) continue;
      core.position.x = lerp(core.position.x, t.pos[0], k);
      core.position.y = lerp(core.position.y, t.pos[1], k);
      core.position.z = lerp(core.position.z, t.pos[2], k);
      core.material.opacity = lerp(core.material.opacity, t.op, k);
      if (t.gone && core.material.opacity < 0.03) {
        world.remove(core);
        coreMap.delete(id);
      }
    }
  };

  // --- Renderer / camera --------------------------------------------------
  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  mount.textContent = "";
  mount.appendChild(renderer.domElement);
  renderer.domElement.style.display = "block";
  renderer.domElement.style.width = "100%";
  renderer.domElement.style.cursor = "grab";
  renderer.domElement.setAttribute("role", "img");
  renderer.domElement.setAttribute("aria-label", "Interactive 3D build of a BORSUK index");

  const camera = new THREE.PerspectiveCamera(44, 1, 0.1, 100);
  const resize = () => {
    const w = mount.clientWidth || 640;
    const h = Math.max(340, Math.round(w * 0.6));
    renderer.setSize(w, h, false);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
  };
  resize();
  if (typeof ResizeObserver !== "undefined") new ResizeObserver(resize).observe(mount);

  let yaw = 0.6;
  let pitch = 0.3;
  let auto = true;
  let drag = false;
  let lx = 0;
  let ly = 0;
  renderer.domElement.addEventListener("pointerdown", (e) => {
    drag = true;
    auto = false;
    lx = e.clientX;
    ly = e.clientY;
    renderer.domElement.style.cursor = "grabbing";
  });
  window.addEventListener("pointermove", (e) => {
    if (!drag) return;
    yaw -= (e.clientX - lx) * 0.008;
    pitch = Math.max(-1.2, Math.min(1.2, pitch - (e.clientY - ly) * 0.008));
    lx = e.clientX;
    ly = e.clientY;
  });
  window.addEventListener("pointerup", () => {
    drag = false;
    renderer.domElement.style.cursor = "grab";
  });

  const R = 13;
  const target = new THREE.Vector3(0, 0, 0);
  const animate = () => {
    if (auto) yaw += 0.0018;
    step();
    camera.position.set(R * Math.cos(pitch) * Math.sin(yaw), R * Math.sin(pitch), R * Math.cos(pitch) * Math.cos(yaw));
    camera.lookAt(target);
    renderer.render(scene, camera);
    requestAnimationFrame(animate);
  };

  // --- Wizard controls ----------------------------------------------------
  const statusEl = document.querySelector("[data-lifecycle-status]");
  const captionEl = document.querySelector("[data-lifecycle-caption]");
  const buttons = [...document.querySelectorAll("[data-life-action]")];

  const updateStatus = () => {
    if (!statusEl) return;
    const live = sim.vectors.filter((v) => v.alive).length;
    const dead = sim.vectors.length - live;
    statusEl.innerHTML =
      `<span><strong>${live}</strong> live vectors</span>` +
      `<span><strong>${dead}</strong> tombstoned</span>` +
      `<span><strong>${sim.segments.length}</strong> segments</span>`;
  };

  const say = (text) => {
    if (captionEl) captionEl.textContent = text;
  };

  const actions = {
    add() {
      addVectors(7);
      syncScene();
      say(
        "New vectors are appended and routed to the nearest segment — the one whose centroid (the dark marker) is closest. A centroid is simply the mean of a segment's vectors: each time one joins, it is recomputed as that running average, so the marker drifts, and the radius grows to reach the farthest member. A vector too far from every centroid opens a new segment.",
      );
    },
    split() {
      const n = splitOversized();
      syncScene();
      say(
        n
          ? `${n} bubble${n > 1 ? "s" : ""} grew past capacity and split into tighter children (2-means), each with its own centroid and radius.`
          : "No bubble is over capacity yet — add more vectors first, then split.",
      );
    },
    delete() {
      const n = deleteSome(4);
      syncScene();
      say(
        `${n} vectors are tombstoned. They still occupy their segment (faded) until a compaction physically reclaims them — deletes are soft and lazy.`,
      );
    },
    compact() {
      compact();
      syncScene();
      say(
        "Compaction drops the tombstoned rows and merges sparse bubbles into their nearest neighbour, repacking the index without touching healthy segments.",
      );
    },
    reset() {
      sim.vectors = [];
      sim.segments = [];
      syncScene();
      say("Empty index. Add vectors to begin.");
    },
  };

  buttons.forEach((b) =>
    b.addEventListener("click", () => {
      const fn = actions[b.dataset.lifeAction];
      if (fn) fn();
    }),
  );

  syncScene();
  say(
    "Empty index. Add vectors to watch BORSUK group them into segments — each summarized by a centroid (the mean of its vectors, shown as a dark marker) and a radius that reaches its farthest member.",
  );
  animate();
}

if (typeof document !== "undefined") {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initLifecycle3d);
  } else {
    initLifecycle3d();
  }
}
