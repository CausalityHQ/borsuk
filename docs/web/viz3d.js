// Real 3D walk-through of a BORSUK query, rendered with three.js.
//
// This module is loaded as an ES module directly by docs.html and is deliberately
// kept out of app.js: the docs web test harness runs app.js in a fake DOM with no
// WebGL, so the heavy 3D path must never be on that code path. If three.js cannot
// be fetched (offline / file://), we leave the static fallback text in place.
//
// The scene mirrors what the engine actually does: vectors are points in space,
// each segment is a bubble with a centroid and a radius sized to its farthest
// member, a query prunes bubbles whose nearest possible point (‖q−centroid‖−r) is
// too far, only the survivors are read, and the read candidates are exact-scored.

const THREE_URL = "https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.module.js";

const PALETTE = {
  segments: [0x2f7f73, 0xc14d32, 0x6f4a31],
  query: 0x26352d,
  paper: 0xf7f7f2,
  ink: 0x1c2520,
  muted: 0x8c9287,
};

// Three well-separated clusters of vectors and one query near the rust cluster.
// Coordinates are hand-placed so the story reads clearly from any angle.
const CLUSTERS = [
  { center: [-3.1, -0.4, 0.6], members: [[0.6, 0.5, 0.4], [-0.5, 0.6, -0.4], [0.4, -0.6, 0.5], [-0.6, -0.4, -0.5], [0.2, 0.3, -0.6]] },
  { center: [2.6, 0.3, 1.2], members: [[0.5, 0.4, 0.4], [-0.4, 0.5, -0.4], [0.4, -0.5, 0.4], [-0.5, -0.3, -0.4], [0.3, 0.2, 0.5]] },
  { center: [0.3, 2.7, -2.4], members: [[0.5, 0.4, 0.5], [-0.5, 0.4, -0.4], [0.4, -0.5, 0.4], [-0.4, -0.4, -0.5]] },
];
const QUERY = [2.0, 0.1, 0.8];

const CAPTIONS = [
  "Every vector is a point in space. Memory holds only a tiny map of where the groups are — never the vectors themselves.",
  "BORSUK groups nearby vectors into segments. Each segment is a bubble: a centroid and a radius sized to reach its farthest member. Bubbles can overlap — a vector belongs to one segment, but the space they cover may not be disjoint.",
  "For a query, each bubble's best possible distance is ‖query − centroid‖ − radius. Bubbles that can't beat the current best are pruned — never read. Where bubbles overlap the query, every one that qualifies is read.",
  null, // step 3 (read) — caption depends on the selected leaf mode
  "The exact-scored candidates are ranked on their full vectors, and the true nearest neighbours are returned. The leaf mode only changes which rows were scored — never the final ranking.",
];

// How the selected leaf mode chooses which rows inside a read bubble get
// exact-scored (the lines from the query). See the Leaf modes section for the
// real algorithms; here the "closest few" stand in for the sketch ranking.
const MODE_READ = {
  "flat-scan": "Only the surviving bubbles are fetched. flat-scan exact-scores every row in them (all the lines) — exact within each bubble.",
  "sq-scan": "Only the surviving bubbles are fetched. sq-scan ranks rows by a scalar code and exact-scores just the closest few (the lines).",
  "pq-scan": "Only the surviving bubbles are fetched. pq-scan ranks rows by a compact per-dimension code and exact-scores just the closest few (the lines).",
  graph: "Only the surviving bubbles are fetched. graph walks a small in-bubble neighbour graph from an entry row (coloured edges), exact-scoring rows as it reaches them.",
};
const CANDIDATE_BUDGET = 4;

function distance(a, b) {
  return Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]);
}

// Centroid + radius per segment, computed from members exactly like the engine.
function buildSegments() {
  return CLUSTERS.map((cluster, index) => {
    const points = cluster.members.map((offset) => offset.map((v, axis) => cluster.center[axis] + v));
    const centroid = [0, 1, 2].map((axis) => points.reduce((sum, p) => sum + p[axis], 0) / points.length);
    const radius = Math.max(...points.map((p) => distance(p, centroid)));
    return { points, centroid, radius, color: PALETTE.segments[index], index };
  });
}

export async function initViz3d() {
  const mount = document.querySelector("[data-viz3d]");
  if (!mount) return;

  let THREE;
  try {
    THREE = await import(/* @vite-ignore */ THREE_URL);
  } catch {
    mount.innerHTML =
      '<p class="viz-fallback">The interactive 3D view needs a network connection to load three.js. The five steps: vectors → bubble into segments → prune by radius → read survivors → exact rerank.</p>';
    return;
  }

  const segments = buildSegments();
  // Route: lower bound = ‖query − centroid‖ − radius; the two lowest are read.
  const ranked = segments
    .map((s) => ({ index: s.index, bound: distance(QUERY, s.centroid) - s.radius }))
    .sort((a, b) => a.bound - b.bound);
  const read = new Set([ranked[0].index, ranked[1].index]);
  const allPoints = segments.flatMap((s) => s.points.map((pos) => ({ pos, segment: s.index })));
  // Rows that live inside a read (fetched) bubble, nearest-first. The leaf mode
  // decides which of these actually get exact-scored.
  const readIndices = allPoints.map((_, i) => i).filter((i) => read.has(allPoints[i].segment));
  const byQueryDistance = (a, b) => distance(allPoints[a].pos, QUERY) - distance(allPoints[b].pos, QUERY);
  const nearestFirst = [...readIndices].sort(byQueryDistance);

  // A tiny neighbour graph over the read rows, walked greedily from the nearest
  // entry — the demo stand-in for the segment-local graph modes.
  const neighbours = (a) =>
    readIndices
      .filter((b) => b !== a)
      .sort((x, y) => distance(allPoints[a].pos, allPoints[x].pos) - distance(allPoints[a].pos, allPoints[y].pos))
      .slice(0, 2);
  const graphCandidates = [];
  const graphWalkEdges = [];
  {
    const visited = new Set();
    const frontier = nearestFirst.length ? [nearestFirst[0]] : [];
    while (graphCandidates.length < CANDIDATE_BUDGET && frontier.length) {
      frontier.sort(byQueryDistance);
      const node = frontier.shift();
      if (visited.has(node)) continue;
      visited.add(node);
      graphCandidates.push(node);
      for (const nb of neighbours(node)) {
        if (!visited.has(nb)) {
          frontier.push(nb);
          graphWalkEdges.push([node, nb]);
        }
      }
    }
  }

  // sq-scan ranks by a single scalar code, so it filters more coarsely than the
  // per-dimension pq code — model that with a 1-D projection so the two modes
  // pick visibly different candidate sets.
  const scalarCode = (p) => p[0] - p[1] + p[2];
  const queryCode = scalarCode(QUERY);
  const sqNearest = [...readIndices].sort(
    (a, b) => Math.abs(scalarCode(allPoints[a].pos) - queryCode) - Math.abs(scalarCode(allPoints[b].pos) - queryCode),
  );

  const candidatesByMode = {
    "flat-scan": readIndices,
    "sq-scan": sqNearest.slice(0, CANDIDATE_BUDGET),
    "pq-scan": nearestFirst.slice(0, CANDIDATE_BUDGET),
    graph: graphCandidates,
  };
  let mode = "pq-scan";
  const candidateSet = () => new Set(candidatesByMode[mode] ?? readIndices);
  // Winners are the two nearest among the rows this mode actually scored.
  const winnersForMode = () => [...(candidatesByMode[mode] ?? readIndices)].sort(byQueryDistance).slice(0, 2);

  const scene = new THREE.Scene();
  const world = new THREE.Group();
  scene.add(world);

  // Lights for soft, real shading on the spheres.
  scene.add(new THREE.HemisphereLight(0xffffff, 0xbfc3b6, 1.1));
  const key = new THREE.DirectionalLight(0xffffff, 1.4);
  key.position.set(4, 6, 5);
  scene.add(key);
  const rim = new THREE.DirectionalLight(0xffe6c8, 0.5);
  rim.position.set(-5, -2, -4);
  scene.add(rim);

  const toVec = (p) => new THREE.Vector3(p[0], p[1], p[2]);

  // --- Point spheres -------------------------------------------------------
  const pointGeo = new THREE.SphereGeometry(0.16, 24, 24);
  const pointMeshes = allPoints.map((p) => {
    const material = new THREE.MeshStandardMaterial({
      color: segments[p.segment].color,
      roughness: 0.35,
      metalness: 0.1,
      // Transparent from the start: toggling `transparent` at runtime needs a
      // shader recompile, so keep it on and only vary `opacity`. Without this
      // the pruned/non-candidate dimming never renders and switching leaf modes
      // looks like it does nothing.
      transparent: true,
    });
    const mesh = new THREE.Mesh(pointGeo, material);
    mesh.position.copy(toVec(p.pos));
    world.add(mesh);
    return mesh;
  });

  // --- Bubble spheres (translucent shell + wireframe) ----------------------
  const bubbles = segments.map((s) => {
    const group = new THREE.Group();
    const geo = new THREE.SphereGeometry(s.radius, 40, 40);
    const shell = new THREE.Mesh(
      geo,
      new THREE.MeshStandardMaterial({
        color: s.color,
        transparent: true,
        opacity: 0.1,
        roughness: 0.6,
        side: THREE.DoubleSide,
        depthWrite: false,
      }),
    );
    const wire = new THREE.LineSegments(
      new THREE.WireframeGeometry(new THREE.SphereGeometry(s.radius, 16, 12)),
      new THREE.LineBasicMaterial({ color: s.color, transparent: true, opacity: 0.28 }),
    );
    const core = new THREE.Mesh(
      new THREE.SphereGeometry(0.09, 16, 16),
      new THREE.MeshBasicMaterial({ color: s.color }),
    );
    group.add(shell, wire, core);
    group.position.copy(toVec(s.centroid));
    group.visible = false;
    world.add(group);
    return { group, shell, wire, core, segment: s };
  });

  // --- Query marker --------------------------------------------------------
  const query = new THREE.Group();
  query.add(
    new THREE.Mesh(
      new THREE.SphereGeometry(0.22, 24, 24),
      new THREE.MeshStandardMaterial({ color: PALETTE.query, roughness: 0.25, emissive: 0x0d1310 }),
    ),
  );
  const halo = new THREE.Mesh(
    new THREE.RingGeometry(0.32, 0.4, 32),
    new THREE.MeshBasicMaterial({ color: PALETTE.query, transparent: true, opacity: 0.6, side: THREE.DoubleSide }),
  );
  query.add(halo);
  query.position.copy(toVec(QUERY));
  query.visible = false;
  world.add(query);

  // --- Score links: query → each row the leaf mode exact-scores ------------
  const scoreLines = readIndices.map((i) => {
    const geometry = new THREE.BufferGeometry().setFromPoints([toVec(QUERY), toVec(allPoints[i].pos)]);
    const line = new THREE.Line(
      geometry,
      new THREE.LineBasicMaterial({ color: PALETTE.ink, transparent: true, opacity: 0.7 }),
    );
    line.visible = false;
    line.userData.index = i;
    world.add(line);
    return line;
  });

  // --- Graph-walk edges (shown only in graph mode) -------------------------
  const graphEdgeLines = graphWalkEdges.map(([a, b]) => {
    const geometry = new THREE.BufferGeometry().setFromPoints([toVec(allPoints[a].pos), toVec(allPoints[b].pos)]);
    const line = new THREE.Line(
      geometry,
      new THREE.LineBasicMaterial({ color: PALETTE.segments[1], transparent: true, opacity: 0.6 }),
    );
    line.visible = false;
    world.add(line);
    return line;
  });

  // Recentre the world so rotation orbits the scene's middle.
  const bounds = new THREE.Box3().setFromObject(world);
  const centre = bounds.getCenter(new THREE.Vector3());
  world.position.sub(centre);

  // --- Renderer / camera ---------------------------------------------------
  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setClearColor(PALETTE.paper, 1);
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  mount.textContent = ""; // drop the "Loading…" placeholder now that WebGL is up
  mount.appendChild(renderer.domElement);
  renderer.domElement.style.display = "block";
  renderer.domElement.style.width = "100%";
  renderer.domElement.style.cursor = "grab";
  renderer.domElement.setAttribute("role", "img");
  renderer.domElement.setAttribute("aria-label", "Interactive 3D view of a BORSUK query");

  const camera = new THREE.PerspectiveCamera(42, 1, 0.1, 100);
  const camRadius = 11;

  const resize = () => {
    const width = mount.clientWidth || 640;
    const height = Math.max(320, Math.round(width * 0.62));
    renderer.setSize(width, height, false);
    camera.aspect = width / height;
    camera.updateProjectionMatrix();
  };
  resize();
  if (typeof ResizeObserver !== "undefined") new ResizeObserver(resize).observe(mount);

  // --- Rotation: gentle auto-orbit + click-drag ----------------------------
  let yaw = 0.7;
  let pitch = 0.32;
  let autoRotate = true;
  let dragging = false;
  let lastX = 0;
  let lastY = 0;

  const onDown = (event) => {
    dragging = true;
    autoRotate = false;
    lastX = event.clientX ?? event.touches?.[0]?.clientX ?? 0;
    lastY = event.clientY ?? event.touches?.[0]?.clientY ?? 0;
    renderer.domElement.style.cursor = "grabbing";
  };
  const onMove = (event) => {
    if (!dragging) return;
    const x = event.clientX ?? event.touches?.[0]?.clientX ?? 0;
    const y = event.clientY ?? event.touches?.[0]?.clientY ?? 0;
    yaw -= (x - lastX) * 0.008;
    pitch = Math.max(-1.2, Math.min(1.2, pitch - (y - lastY) * 0.008));
    lastX = x;
    lastY = y;
  };
  const onUp = () => {
    dragging = false;
    renderer.domElement.style.cursor = "grab";
  };
  renderer.domElement.addEventListener("pointerdown", onDown);
  window.addEventListener("pointermove", onMove);
  window.addEventListener("pointerup", onUp);

  // --- Step state machine --------------------------------------------------
  const setOpacity = (material, value) => {
    material.opacity = value;
    material.transparent = value < 1;
  };

  let step = 0;
  const applyStep = (next) => {
    step = next;
    const cands = candidateSet();
    const winners = winnersForMode();
    const scored = step >= 3; // rows get exact-scored from the "read" step on

    // Points fade when their segment is pruned / not read. Scored candidates
    // brighten; the final winners are enlarged.
    pointMeshes.forEach((mesh, i) => {
      const inRead = read.has(allPoints[i].segment);
      const isCand = scored && cands.has(i);
      let opacity = 1;
      if (step === 2) opacity = inRead ? 1 : 0.28;
      if (step >= 3) opacity = inRead ? (isCand ? 1 : 0.5) : 0.12;
      // Material is already transparent; vary opacity only (no transparent toggle).
      mesh.material.opacity = opacity;
      const winner = step >= 4 && winners.includes(i);
      mesh.scale.setScalar(winner ? 1.7 : isCand ? 1.2 : 1);
      mesh.material.emissive?.setHex(winner ? 0x26352d : 0x000000);
    });

    // Bubbles appear at step 1; pruned ones dim from step 2 on.
    bubbles.forEach((bubble) => {
      const isRead = read.has(bubble.segment.index);
      bubble.group.visible = step >= 1;
      const pruned = step >= 2 && !isRead;
      setOpacity(bubble.shell.material, pruned ? 0.03 : step >= 2 && isRead ? 0.16 : 0.1);
      setOpacity(bubble.wire.material, pruned ? 0.08 : step >= 2 && isRead ? 0.5 : 0.28);
      setOpacity(bubble.core.material, pruned ? 0.25 : 1);
    });

    query.visible = step >= 2;
    // One line per scored candidate; winners drawn darker.
    scoreLines.forEach((line) => {
      const isCand = scored && cands.has(line.userData.index);
      line.visible = isCand;
      const winner = step >= 4 && winners.includes(line.userData.index);
      line.material.color.setHex(winner ? PALETTE.query : PALETTE.ink);
      setOpacity(line.material, winner ? 0.95 : 0.65);
    });
    graphEdgeLines.forEach((line) => (line.visible = scored && mode === "graph"));
  };

  const buttons = [...document.querySelectorAll("[data-viz-step]")];
  const modeButtons = [...document.querySelectorAll("[data-viz-mode]")];
  const caption = document.querySelector("[data-viz-caption]");
  const captionFor = (s) => (s === 3 ? MODE_READ[mode] : CAPTIONS[s]);
  const select = (next) => {
    applyStep(next);
    if (caption) caption.textContent = captionFor(next);
    buttons.forEach((b) => b.classList.toggle("is-active", Number(b.dataset.vizStep) === next));
  };
  buttons.forEach((b) => b.addEventListener("click", () => select(Number(b.dataset.vizStep))));
  modeButtons.forEach((b) =>
    b.addEventListener("click", () => {
      mode = b.dataset.vizMode;
      modeButtons.forEach((x) => x.classList.toggle("is-active", x.dataset.vizMode === mode));
      // Jump to the "read" step so the mode's effect is visible immediately.
      select(step >= 3 ? step : 3);
    }),
  );
  modeButtons.forEach((b) => b.classList.toggle("is-active", b.dataset.vizMode === mode));
  select(0);

  // --- Render loop ---------------------------------------------------------
  const target = new THREE.Vector3(0, 0, 0);
  const animate = () => {
    if (autoRotate) yaw += 0.0022;
    camera.position.set(
      camRadius * Math.cos(pitch) * Math.sin(yaw),
      camRadius * Math.sin(pitch),
      camRadius * Math.cos(pitch) * Math.cos(yaw),
    );
    camera.lookAt(target);
    // Billboard the query halo toward the camera.
    halo.lookAt(camera.position);
    renderer.render(scene, camera);
    requestAnimationFrame(animate);
  };
  animate();
}

if (typeof document !== "undefined") {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initViz3d);
  } else {
    initViz3d();
  }
}
