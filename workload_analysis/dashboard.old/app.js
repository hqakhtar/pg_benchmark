"use strict";

const REQUIRED_COLUMNS = [
  "collected_at",
  "oltp_pct",
  "olap_pct",
  "htap_pct",
  "timeseries_pct",
  "dominant_workload",
  "scale_recommendation",
];

const SERIES = [
  { key: "oltp_pct", label: "OLTP", color: "#16766f" },
  { key: "olap_pct", label: "OLAP", color: "#d75b42" },
  { key: "htap_pct", label: "HTAP", color: "#c58a16" },
  { key: "timeseries_pct", label: "Time series", color: "#3d67ac" },
];

const VERDICTS = {
  SCALE_OUT: { label: "Scale out", tone: "positive" },
  SCALE_UP_OR_TUNE: { label: "Scale up or tune", tone: "warning" },
  REBALANCE_FIRST: { label: "Rebalance first", tone: "critical" },
  OPTIMIZE_LOCALITY_FIRST: { label: "Optimize locality", tone: "critical" },
  NO_DATA: { label: "No data", tone: "neutral" },
  REVIEW: { label: "Review", tone: "warning" },
};

const state = {
  rows: [],
  sourceName: "",
  sourceKind: "",
  range: "24h",
  trendRows: [],
};

const elements = {
  sourceName: document.querySelector("#sourceName"),
  sourceState: document.querySelector(".source-state"),
  sourceFootnote: document.querySelector("#sourceFootnote"),
  refreshButton: document.querySelector("#refreshButton"),
  importButton: document.querySelector("#importButton"),
  fileInput: document.querySelector("#fileInput"),
  notice: document.querySelector("#notice"),
  snapshotTitle: document.querySelector("#snapshotTitle"),
  captureMeta: document.querySelector("#captureMeta"),
  dominantValue: document.querySelector("#dominantValue"),
  localValue: document.querySelector("#localValue"),
  routabilityValue: document.querySelector("#routabilityValue"),
  fairnessValue: document.querySelector("#fairnessValue"),
  mixDonut: document.querySelector("#mixDonut"),
  donutValue: document.querySelector("#donutValue"),
  donutLabel: document.querySelector("#donutLabel"),
  mixLegend: document.querySelector("#mixLegend"),
  decisionBadge: document.querySelector("#decisionBadge"),
  decisionText: document.querySelector("#decisionText"),
  analyticalValue: document.querySelector("#analyticalValue"),
  crossNodeValue: document.querySelector("#crossNodeValue"),
  captureCount: document.querySelector("#captureCount"),
  trendChart: document.querySelector("#trendChart"),
  chartTooltip: document.querySelector("#chartTooltip"),
  localBarValue: document.querySelector("#localBarValue"),
  localBar: document.querySelector("#localBar"),
  crossBarValue: document.querySelector("#crossBarValue"),
  crossBar: document.querySelector("#crossBar"),
  routeBarValue: document.querySelector("#routeBarValue"),
  routeBar: document.querySelector("#routeBar"),
  fairnessGauge: document.querySelector("#fairnessGauge"),
  fairnessGaugeValue: document.querySelector("#fairnessGaugeValue"),
  fairnessState: document.querySelector("#fairnessState"),
  fairnessDetail: document.querySelector("#fairnessDetail"),
  historyRange: document.querySelector("#historyRange"),
  historyBody: document.querySelector("#historyBody"),
  dropOverlay: document.querySelector("#dropOverlay"),
};

function parseCSV(text) {
  const records = [];
  let record = [];
  let field = "";
  let quoted = false;
  const source = text.replace(/^\uFEFF/, "");

  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];

    if (quoted) {
      if (character === '"' && source[index + 1] === '"') {
        field += '"';
        index += 1;
      } else if (character === '"') {
        quoted = false;
      } else {
        field += character;
      }
      continue;
    }

    if (character === '"') {
      quoted = true;
    } else if (character === ",") {
      record.push(field);
      field = "";
    } else if (character === "\n") {
      record.push(field);
      if (record.some((value) => value.trim() !== "")) records.push(record);
      record = [];
      field = "";
    } else if (character !== "\r") {
      field += character;
    }
  }

  if (quoted) throw new Error("CSV contains an unterminated quoted field.");
  record.push(field);
  if (record.some((value) => value.trim() !== "")) records.push(record);
  if (records.length < 2) throw new Error("CSV must contain a header and at least one capture.");

  const headers = records[0].map((value) => value.trim());
  const missing = REQUIRED_COLUMNS.filter((column) => !headers.includes(column));
  if (missing.length > 0) throw new Error(`CSV is missing required columns: ${missing.join(", ")}.`);

  return records.slice(1).map((values) => {
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] === undefined ? "" : values[index].trim();
    });
    return row;
  });
}

function numericValue(value) {
  if (value === undefined || value === null || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function normalizeRows(records) {
  const numericColumns = [
    "oltp_pct",
    "olap_pct",
    "htap_pct",
    "timeseries_pct",
    "data_distribution_fairness",
    "ideal_local_exec_pct",
    "cross_node_exec_pct",
    "mean_routability_score",
  ];

  const rows = records.map((record, index) => {
    const capturedAt = new Date(record.collected_at);
    if (Number.isNaN(capturedAt.getTime())) {
      throw new Error(`Capture ${index + 1} has an invalid collected_at value.`);
    }

    const row = { ...record, capturedAt };
    numericColumns.forEach((column) => {
      row[column] = numericValue(record[column]);
    });
    row.dominant_workload = record.dominant_workload.toUpperCase();
    return row;
  });

  return rows.sort((left, right) => left.capturedAt - right.capturedAt);
}

function formatPercent(value, digits = 1) {
  return value === null ? "N/A" : `${value.toFixed(digits)}%`;
}

function formatScore(value) {
  return value === null ? "N/A" : value.toFixed(2);
}

function formatCapture(date, includeDate = true) {
  const options = includeDate
    ? { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }
    : { hour: "2-digit", minute: "2-digit" };
  return new Intl.DateTimeFormat(undefined, options).format(date);
}

function getVerdict(recommendation) {
  const key = (recommendation || "REVIEW").split(":", 1)[0].trim();
  return { key, ...(VERDICTS[key] || VERDICTS.REVIEW) };
}

function recommendationDetail(recommendation) {
  const separator = recommendation.indexOf(":");
  if (separator === -1) return recommendation || "No recommendation was reported.";
  const detail = recommendation.slice(separator + 1).trim();
  return detail.charAt(0).toUpperCase() + detail.slice(1);
}

function setNotice(message = "") {
  elements.notice.hidden = !message;
  elements.notice.textContent = message;
}

function setSource(name, kind, failed = false) {
  state.sourceName = name;
  state.sourceKind = kind;
  elements.sourceName.textContent = name;
  elements.sourceState.classList.toggle("ready", !failed);
  elements.sourceState.classList.toggle("error", failed);
  elements.sourceFootnote.textContent = `${kind} / ${name}`;
}

function renderMix(latest) {
  const values = SERIES.map((series) => Math.max(0, latest[series.key] || 0));
  const total = values.reduce((sum, value) => sum + value, 0) || 1;
  let cursor = 0;
  const stops = SERIES.map((series, index) => {
    const start = cursor;
    cursor += (values[index] / total) * 100;
    return `${series.color} ${start.toFixed(2)}% ${cursor.toFixed(2)}%`;
  });
  elements.mixDonut.style.background = `conic-gradient(${stops.join(", ")})`;

  const dominantKey = `${latest.dominant_workload.toLowerCase()}_pct`.replace("time_series", "timeseries");
  const dominantValue = latest[dominantKey];
  elements.donutValue.textContent = formatPercent(dominantValue);
  elements.donutLabel.textContent = latest.dominant_workload.replace("_", " ");
  elements.mixDonut.setAttribute(
    "aria-label",
    SERIES.map((series) => `${series.label} ${formatPercent(latest[series.key])}`).join(", "),
  );

  elements.mixLegend.replaceChildren();
  SERIES.forEach((series) => {
    const row = document.createElement("div");
    row.className = "legend-row";
    const swatch = document.createElement("i");
    swatch.style.backgroundColor = series.color;
    const label = document.createElement("span");
    label.textContent = series.label;
    const value = document.createElement("strong");
    value.textContent = formatPercent(latest[series.key]);
    row.append(swatch, label, value);
    elements.mixLegend.append(row);
  });
}

function renderDecision(latest) {
  const verdict = getVerdict(latest.scale_recommendation);
  elements.decisionBadge.textContent = verdict.label;
  elements.decisionBadge.className = `decision-badge ${verdict.tone}`;
  elements.decisionText.textContent = recommendationDetail(latest.scale_recommendation);
  elements.analyticalValue.textContent = formatPercent((latest.olap_pct || 0) + (latest.htap_pct || 0));
  elements.crossNodeValue.textContent = formatPercent(latest.cross_node_exec_pct);
  elements.captureCount.textContent = state.rows.length.toLocaleString();
}

function filterTrendRows() {
  if (state.range === "all" || state.rows.length === 0) return state.rows;
  const latestTime = state.rows[state.rows.length - 1].capturedAt.getTime();
  const duration = state.range === "24h" ? 24 * 60 * 60 * 1000 : 7 * 24 * 60 * 60 * 1000;
  const filtered = state.rows.filter((row) => row.capturedAt.getTime() >= latestTime - duration);
  return filtered.length > 0 ? filtered : state.rows.slice(-1);
}

function svgElement(name, attributes = {}) {
  const element = document.createElementNS("http://www.w3.org/2000/svg", name);
  Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, value));
  return element;
}

function renderTrend() {
  const rows = filterTrendRows();
  state.trendRows = rows;
  const svg = elements.trendChart;
  svg.replaceChildren();

  const width = 920;
  const height = 280;
  const plot = { left: 48, right: 900, top: 18, bottom: 242 };
  const plotWidth = plot.right - plot.left;
  const plotHeight = plot.bottom - plot.top;
  const xAt = (index) => plot.left + (rows.length === 1 ? plotWidth / 2 : (index / (rows.length - 1)) * plotWidth);
  const yAt = (value) => plot.bottom - (Math.max(0, Math.min(100, value || 0)) / 100) * plotHeight;

  [0, 25, 50, 75, 100].forEach((tick) => {
    const y = yAt(tick);
    svg.append(svgElement("line", { x1: plot.left, y1: y, x2: plot.right, y2: y, class: "grid-line" }));
    const label = svgElement("text", { x: 8, y: y + 3, class: "axis-label" });
    label.textContent = `${tick}%`;
    svg.append(label);
  });

  if (rows.length === 0) return;

  SERIES.forEach((series) => {
    const points = rows.map((row, index) => [xAt(index), yAt(row[series.key])]);
    const pathData = points.map(([x, y], index) => `${index === 0 ? "M" : "L"} ${x} ${y}`).join(" ");
    const areaData = `${pathData} L ${points[points.length - 1][0]} ${plot.bottom} L ${points[0][0]} ${plot.bottom} Z`;
    svg.append(svgElement("path", { d: areaData, fill: series.color, class: "trend-area" }));
    svg.append(svgElement("path", { d: pathData, stroke: series.color, class: "trend-line" }));
    points.forEach(([x, y]) => {
      svg.append(svgElement("circle", { cx: x, cy: y, r: 3.5, fill: series.color, class: "trend-dot" }));
    });
  });

  const labelIndexes = [...new Set([0, Math.floor((rows.length - 1) / 2), rows.length - 1])];
  labelIndexes.forEach((index) => {
    const label = svgElement("text", {
      x: xAt(index),
      y: 270,
      class: "axis-label",
      "text-anchor": index === 0 ? "start" : index === rows.length - 1 ? "end" : "middle",
    });
    label.textContent = formatCapture(rows[index].capturedAt);
    svg.append(label);
  });

  svg.append(svgElement("line", {
    id: "hoverLine",
    x1: plot.left,
    y1: plot.top,
    x2: plot.left,
    y2: plot.bottom,
    class: "hover-line",
    visibility: "hidden",
  }));
}

function renderRouting(latest) {
  const metrics = [
    [elements.localBarValue, elements.localBar, latest.ideal_local_exec_pct, 100, true],
    [elements.crossBarValue, elements.crossBar, latest.cross_node_exec_pct, 100, true],
    [elements.routeBarValue, elements.routeBar, latest.mean_routability_score, 1, false],
  ];

  metrics.forEach(([label, bar, value, maximum, percent]) => {
    label.textContent = percent ? formatPercent(value) : formatScore(value);
    bar.style.width = value === null ? "0%" : `${Math.max(0, Math.min(100, (value / maximum) * 100))}%`;
  });
}

function renderFairness(latest) {
  const fairness = latest.data_distribution_fairness;
  elements.fairnessGaugeValue.textContent = formatScore(fairness);
  elements.fairnessGauge.style.setProperty("--fairness", `${Math.max(0, Math.min(1, fairness || 0)) * 360}deg`);

  if (fairness === null) {
    elements.fairnessState.textContent = "Not reported";
    elements.fairnessDetail.textContent = "Citus shard metrics are absent from this capture.";
    elements.fairnessGauge.setAttribute("aria-label", "Shard fairness not available");
  } else if (fairness >= 0.85) {
    elements.fairnessState.textContent = "Balanced";
    elements.fairnessDetail.textContent = "Shard bytes are distributed evenly enough for scale-out.";
    elements.fairnessGauge.setAttribute("aria-label", `Shard fairness ${fairness.toFixed(2)}, balanced`);
  } else {
    elements.fairnessState.textContent = "Skew detected";
    elements.fairnessDetail.textContent = "Shard byte distribution needs attention before adding workers.";
    elements.fairnessGauge.setAttribute("aria-label", `Shard fairness ${fairness.toFixed(2)}, skew detected`);
  }
}

function appendCell(row, value, className = "") {
  const cell = document.createElement("td");
  if (className) {
    const tag = document.createElement("span");
    tag.className = className;
    tag.textContent = value;
    cell.append(tag);
  } else {
    cell.textContent = value;
  }
  row.append(cell);
}

function renderHistory() {
  elements.historyBody.replaceChildren();
  const rows = state.rows.slice(-8).reverse();
  rows.forEach((capture) => {
    const row = document.createElement("tr");
    const workloadClass = `${capture.dominant_workload.toLowerCase().replace("_", "")}tag`;
    const verdict = getVerdict(capture.scale_recommendation);
    appendCell(row, formatCapture(capture.capturedAt));
    appendCell(row, capture.dominant_workload.replace("_", " "), `workload-tag ${workloadClass}`);
    appendCell(row, formatPercent(capture.oltp_pct));
    appendCell(row, formatPercent(capture.olap_pct));
    appendCell(row, formatPercent(capture.htap_pct));
    appendCell(row, formatPercent(capture.timeseries_pct));
    appendCell(row, formatPercent(capture.ideal_local_exec_pct));
    appendCell(row, verdict.label, "table-decision");
    elements.historyBody.append(row);
  });

  const first = state.rows[0].capturedAt;
  const last = state.rows[state.rows.length - 1].capturedAt;
  elements.historyRange.textContent = `${formatCapture(first)} - ${formatCapture(last)}`;
}

function render() {
  if (state.rows.length === 0) return;
  const latest = state.rows[state.rows.length - 1];
  elements.snapshotTitle.textContent = `${latest.dominant_workload.replace("_", " ")} leads the current profile`;
  elements.captureMeta.textContent = `${formatCapture(latest.capturedAt)} / ${state.rows.length.toLocaleString()} observations`;
  elements.dominantValue.textContent = latest.dominant_workload.replace("_", " ");
  elements.localValue.textContent = formatPercent(latest.ideal_local_exec_pct);
  elements.routabilityValue.textContent = formatScore(latest.mean_routability_score);
  elements.fairnessValue.textContent = formatScore(latest.data_distribution_fairness);
  renderMix(latest);
  renderDecision(latest);
  renderTrend();
  renderRouting(latest);
  renderFairness(latest);
  renderHistory();
}

function loadText(text, sourceName, sourceKind) {
  const records = parseCSV(text);
  state.rows = normalizeRows(records);
  setSource(sourceName, sourceKind);
  setNotice();
  render();
}

async function loadDefaultSource() {
  elements.refreshButton.disabled = true;
  elements.sourceName.textContent = "Locating source";
  elements.sourceState.classList.remove("ready", "error");

  try {
    const response = await fetch("../workload_score.csv", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    loadText(await response.text(), "workload_score.csv", "Classifier export");
  } catch (sourceError) {
    try {
      const response = await fetch("sample-workload.csv", { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      loadText(await response.text(), "sample-workload.csv", "Sample source");
      setNotice("The classifier export was not found, so the dashboard is showing sample captures.");
    } catch (sampleError) {
      setSource("Source unavailable", "No data", true);
      setNotice("Could not load a workload CSV. Import a result-set 9 export to continue.");
    }
  } finally {
    elements.refreshButton.disabled = false;
  }
}

async function loadFile(file) {
  if (!file) return;
  try {
    loadText(await file.text(), file.name, "Imported file");
  } catch (error) {
    setSource(file.name, "Invalid file", true);
    setNotice(error.message);
  } finally {
    elements.fileInput.value = "";
  }
}

function showChartTooltip(event) {
  if (state.trendRows.length === 0) return;
  const bounds = elements.trendChart.getBoundingClientRect();
  const viewX = ((event.clientX - bounds.left) / bounds.width) * 920;
  const plotLeft = 48;
  const plotRight = 900;
  const clampedX = Math.max(plotLeft, Math.min(plotRight, viewX));
  const index = state.trendRows.length === 1
    ? 0
    : Math.round(((clampedX - plotLeft) / (plotRight - plotLeft)) * (state.trendRows.length - 1));
  const row = state.trendRows[index];
  const pointX = state.trendRows.length === 1
    ? (plotLeft + plotRight) / 2
    : plotLeft + (index / (state.trendRows.length - 1)) * (plotRight - plotLeft);
  const hoverLine = document.querySelector("#hoverLine");
  if (hoverLine) {
    hoverLine.setAttribute("x1", pointX);
    hoverLine.setAttribute("x2", pointX);
    hoverLine.setAttribute("visibility", "visible");
  }

  elements.chartTooltip.replaceChildren();
  const title = document.createElement("strong");
  title.textContent = formatCapture(row.capturedAt);
  elements.chartTooltip.append(title);
  SERIES.forEach((series) => {
    const line = document.createElement("span");
    const label = document.createElement("b");
    label.textContent = series.label;
    const value = document.createElement("em");
    value.textContent = formatPercent(row[series.key]);
    line.append(label, value);
    elements.chartTooltip.append(line);
  });

  elements.chartTooltip.hidden = false;
  const tooltipWidth = 180;
  const left = Math.min(bounds.width - tooltipWidth - 8, Math.max(8, event.clientX - bounds.left + 14));
  const top = Math.max(8, event.clientY - bounds.top - 52);
  elements.chartTooltip.style.left = `${left}px`;
  elements.chartTooltip.style.top = `${top}px`;
}

function hideChartTooltip() {
  elements.chartTooltip.hidden = true;
  const hoverLine = document.querySelector("#hoverLine");
  if (hoverLine) hoverLine.setAttribute("visibility", "hidden");
}

elements.importButton.addEventListener("click", () => elements.fileInput.click());
elements.fileInput.addEventListener("change", () => loadFile(elements.fileInput.files[0]));
elements.refreshButton.addEventListener("click", loadDefaultSource);

document.querySelectorAll(".range-tabs button").forEach((button) => {
  button.addEventListener("click", () => {
    state.range = button.dataset.range;
    document.querySelectorAll(".range-tabs button").forEach((tab) => {
      tab.setAttribute("aria-selected", String(tab === button));
    });
    renderTrend();
  });
});

elements.trendChart.addEventListener("pointermove", showChartTooltip);
elements.trendChart.addEventListener("pointerleave", hideChartTooltip);

let dragDepth = 0;
window.addEventListener("dragenter", (event) => {
  event.preventDefault();
  dragDepth += 1;
  elements.dropOverlay.classList.add("visible");
});
window.addEventListener("dragover", (event) => event.preventDefault());
window.addEventListener("dragleave", (event) => {
  event.preventDefault();
  dragDepth -= 1;
  if (dragDepth <= 0) {
    dragDepth = 0;
    elements.dropOverlay.classList.remove("visible");
  }
});
window.addEventListener("drop", (event) => {
  event.preventDefault();
  dragDepth = 0;
  elements.dropOverlay.classList.remove("visible");
  loadFile(event.dataTransfer.files[0]);
});

loadDefaultSource();