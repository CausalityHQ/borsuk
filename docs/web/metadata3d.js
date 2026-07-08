// Real 3D walk-through of a *filtered* BORSUK query, rendered with three.js.
//
// Companion to viz3d.js. Where that module shows how bubbles are pruned by
// distance, this one shows the metadata dimension: every vector carries a
// small attribute set, a filter rejects the rows that do not match, and the
// prefilter ranks only the surviving matches. The teaching point is that
// metadata is ORTHOGONAL to position -- a genre is spread across every spatial
// cluster -- so a filter is not the same as spatial proximity, and BORSUK finds
// the matching neighbours wherever they sit.
//
// Loaded as an ES module by docs.html; kept off app.js so the headless docs-web
// test never touches WebGL. If three.js cannot be fetched we leave the fallback.

const THREE_URL = "https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.module.js";

const PALETTE = {
  paper: 0xf7f7f2,
  ink: 0x1c2520,
  query: 0x26352d,
  // Genre colours, reused as the legend in the section prose.
  rock: 0x2f7f73,
  jazz: 0xc14d32,
  pop: 0x6f4a31,
};

const GENRE_COLOR = { rock: PALETTE.rock, jazz: PALETTE.jazz, pop: PALETTE.pop };

// Fifteen vectors in three spatial clusters. Genre and year are assigned so that
// every genre appears in every cluster -- metadata does not follow position.
const CLUSTERS = [
  [-3.0, -0.3, 0.4],
  [2.7, 0.4, 1.1],
  [0.2, 2.6, -2.2],
];
const OFFSETS = [
  [0.6, 0.5, 0.4],
  [-0.5, 0.6, -0.4],
  [0.4, -0.6, 0.5],
  [-0.6, -0.4, -0.5],
  [0.25, 0.3, -0.55],
];
const GENRES = ["rock", "jazz", "pop"];
const POINTS = [];
for (let c = 0; c < CLUSTERS.length; c += 1) {
  for (let i = 0; i < OFFSETS.length; i += 1) {
    const idx = c * OFFSETS.length + i;
    POINTS.push({
      pos: [
        CLUSTERS[c][0] + OFFSETS[i][0],
        CLUSTERS[c][1] + OFFSETS[i][1],
        CLUSTERS[c][2] + OFFSETS[i][2],
      ],
      genre: GENRES[(idx * 2 + c) % GENRES.length],
      year: 1998 + ((idx * 7) % 26), // 1998..2023
    });
  }
}
const QUERY = [2.1, 0.2, 0.7];

const CANDIDATE_BUDGET = 3; // matches the query returns (top-k)

// Each filter is a predicate over a point's metadata plus a human label.
const FILTERS = {
  rock: { label: 'genre = "rock"', test: (p) => p.genre === "rock" },
  jazz: { label: 'genre = "jazz"', test: (p) => p.genre === "jazz" },
  recent: { label: "year ≥ 2010", test: (p) => p.year >= 2010 },
  none: { label: "no filter", test: () => true },
};

const CAPTIONS = [
  "Every vector carries metadata — here a genre and a year. Colour shows the genre. Notice the genres are mixed through all three spatial clusters: metadata does not follow position.",
  null, // step 1 — depends on the selected filter
  "Now BORSUK ranks only the matching rows — a score line runs from the query to each surviving vector. Because rejected rows are filtered out before ranking (never scored), a selective filter does far less work, and the matches it ranks can sit in any cluster.",
  "The nearest matching vectors are returned (enlarged). Because the filter runs before ranking, the result is the true nearest neighbours among the matches — never a nearest-neighbour list with the non-matches quietly dropped afterwards.",
];

function distance(a, b) {
  return Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]);
}

async function initMetadata3d() {
  const mount = document.querySelector("[data-metadata3d]");
  if (!mount) return;
  let THREE;
  try {
    THREE = await import(THREE_URL);
  } catch {
    return; // keep the static fallback text
  }

  let filterKey = "rock";
  const matchIndices = () => POINTS.map((_, i) => i).filter((i) => FILTERS[filterKey].test(POINTS[i]));
  const byQueryDistance = (a, b) => distance(POINTS[a].pos, QUERY) - distance(POINTS[b].pos, QUERY);
  const winnersForFilter = () => [...matchIndices()].sort(byQueryDistance).slice(0, CANDIDATE_BUDGET);

  const scene = new THREE.Scene();
  const world = new THREE.Group();
  scene.add(world);
  scene.add(new THREE.HemisphereLight(0xffffff, 0xbfc3b6, 1.1));
  const key = new THREE.DirectionalLight(0xffffff, 1.4);
  key.position.set(4, 6, 5);
  scene.add(key);

  const toVec = (p) => new THREE.Vector3(p[0], p[1], p[2]);

  const pointGeo = new THREE.SphereGeometry(0.17, 24, 24);
  const pointMeshes = POINTS.map((p) => {
    const mesh = new THREE.Mesh(
      pointGeo,
      // transparent from the start: toggling `transparent` at runtime needs a
      // shader recompile, so we keep it on and only vary `opacity` when a row is
      // rejected by the filter.
      new THREE.MeshStandardMaterial({
        color: GENRE_COLOR[p.genre],
        roughness: 0.35,
        metalness: 0.1,
        transparent: true,
      }),
    );
    mesh.position.copy(toVec(p.pos));
    world.add(mesh);
    return mesh;
  });

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
  world.add(query);

  // One score line per vector; shown only for matching rows from the prefilter
  // step on. Rebuilt is unnecessary — visibility/colour is toggled per step.
  const scoreLines = POINTS.map((p, i) => {
    const line = new THREE.Line(
      new THREE.BufferGeometry().setFromPoints([toVec(QUERY), toVec(p.pos)]),
      new THREE.LineBasicMaterial({ color: PALETTE.ink, transparent: true, opacity: 0.7 }),
    );
    line.visible = false;
    line.userData.index = i;
    world.add(line);
    return line;
  });

  const bounds = new THREE.Box3().setFromObject(world);
  world.position.sub(bounds.getCenter(new THREE.Vector3()));

  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setClearColor(PALETTE.paper, 1);
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  mount.textContent = "";
  mount.appendChild(renderer.domElement);
  renderer.domElement.style.display = "block";
  renderer.domElement.style.width = "100%";
  renderer.domElement.style.cursor = "grab";
  renderer.domElement.setAttribute("role", "img");
  renderer.domElement.setAttribute("aria-label", "Interactive 3D view of a filtered BORSUK query");

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

  let yaw = 0.7;
  let pitch = 0.32;
  let autoRotate = true;
  let dragging = false;
  let lastX = 0;
  let lastY = 0;
  renderer.domElement.addEventListener("pointerdown", (event) => {
    dragging = true;
    autoRotate = false;
    lastX = event.clientX;
    lastY = event.clientY;
    renderer.domElement.style.cursor = "grabbing";
  });
  window.addEventListener("pointermove", (event) => {
    if (!dragging) return;
    yaw -= (event.clientX - lastX) * 0.008;
    pitch = Math.max(-1.2, Math.min(1.2, pitch - (event.clientY - lastY) * 0.008));
    lastX = event.clientX;
    lastY = event.clientY;
  });
  window.addEventListener("pointerup", () => {
    dragging = false;
    renderer.domElement.style.cursor = "grab";
  });

  const setOpacity = (material, value) => {
    material.opacity = value;
    material.transparent = value < 1;
  };

  let step = 0;
  const applyStep = (next) => {
    step = next;
    const matches = new Set(matchIndices());
    const winners = winnersForFilter();
    const filtered = step >= 1; // rejected rows fade from the filter step on
    const scored = step >= 2; // score lines appear at the prefilter step

    pointMeshes.forEach((mesh, i) => {
      const isMatch = matches.has(i);
      // Material is already transparent; vary opacity only (no transparent toggle).
      mesh.material.opacity = filtered && !isMatch ? 0.1 : 1;
      const winner = step >= 3 && winners.includes(i);
      mesh.scale.setScalar(winner ? 1.8 : filtered && isMatch ? 1.2 : 1);
      mesh.material.emissive?.setHex(winner ? 0x26352d : 0x000000);
    });

    scoreLines.forEach((line) => {
      const isMatch = matches.has(line.userData.index);
      line.visible = scored && isMatch;
      const winner = step >= 3 && winners.includes(line.userData.index);
      line.material.color.setHex(winner ? PALETTE.query : PALETTE.ink);
      setOpacity(line.material, winner ? 0.95 : 0.6);
    });
  };

  const stepButtons = [...document.querySelectorAll("[data-mviz-step]")];
  const filterButtons = [...document.querySelectorAll("[data-mviz-filter]")];
  const caption = document.querySelector("[data-mviz-caption]");
  const captionFor = (s) => {
    if (s === 1) {
      const kept = matchIndices().length;
      return `The filter ${FILTERS[filterKey].label} rejects the rows that do not match — they fade out and are never ranked. ${kept} of ${POINTS.length} vectors survive here. Change the filter to see the surviving set shift, independent of where the vectors sit.`;
    }
    return CAPTIONS[s];
  };
  const select = (next) => {
    applyStep(next);
    if (caption) caption.textContent = captionFor(next);
    stepButtons.forEach((b) => b.classList.toggle("is-active", Number(b.dataset.mvizStep) === next));
  };
  stepButtons.forEach((b) => b.addEventListener("click", () => select(Number(b.dataset.mvizStep))));
  filterButtons.forEach((b) =>
    b.addEventListener("click", () => {
      filterKey = b.dataset.mvizFilter;
      filterButtons.forEach((x) => x.classList.toggle("is-active", x.dataset.mvizFilter === filterKey));
      // Jump to the filter step so the change is visible immediately.
      select(step >= 1 ? step : 1);
    }),
  );
  filterButtons.forEach((b) => b.classList.toggle("is-active", b.dataset.mvizFilter === filterKey));
  select(0);

  const target = new THREE.Vector3(0, 0, 0);
  const animate = () => {
    if (autoRotate) yaw += 0.0022;
    camera.position.set(
      camRadius * Math.cos(pitch) * Math.sin(yaw),
      camRadius * Math.sin(pitch),
      camRadius * Math.cos(pitch) * Math.cos(yaw),
    );
    camera.lookAt(target);
    halo.lookAt(camera.position);
    renderer.render(scene, camera);
    requestAnimationFrame(animate);
  };
  animate();
}

if (typeof document !== "undefined") {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initMetadata3d);
  } else {
    initMetadata3d();
  }
}
