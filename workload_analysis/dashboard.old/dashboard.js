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
  {
    key: "oltp_pct",
    focusKey: "oltp",
    dominantKey: "OLTP",
    label: "OLTP",
    detail: "Short, transactional query activity",
    cellClass: "oltp-cell",
  },
  {
    key: "olap_pct",
    focusKey: "olap",
    dominantKey: "OLAP",
    label: "OLAP",
    detail: "Analytical scans and aggregation",
    cellClass: "olap-cell",
  },
  {
    key: "htap_pct",
    focusKey: "htap",
    dominantKey: "HTAP",
    label: "HTAP",
    detail: "Mixed transactional and analytical work",
    cellClass: "htap-cell",
  },
  {
    key: "timeseries_pct",
    focusKey: "timeseries",
    dominantKey: "TIME_SERIES",
    label: "Time series",
    detail: "Time-window and append-oriented access",
    cellClass: "timeseries-cell",
  },
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
  selectedIndex: -1,
  range: "24h",
  emphasis: "all",
  sourceName: "",
  sourceKind: "",
};

const elements = {
  dataBanner: document.querySelector("#dataBanner"),
  bannerLabel: document.querySelector("#bannerLabel"),
  bannerDetail: document.querySelector("#bannerDetail"),
  connectionState: document.querySelector(".connection-state"),
  connectionLabel: document.querySelector("#connectionLabel"),
  sourceName: document.querySelector("#sourceName"),
  sourceKind: document.querySelector("#sourceKind"),
  sourceFootnote: document.querySelector("#sourceFootnote"),
  refreshButton: document.querySelector("#refreshButton"),
  importButton: document.querySelector("#importButton"),
  fileInput: document.querySelector("#fileInput"),
  captureSelect: document.querySelector("#captureSelect"),
  rangeSelect: document.querySelector("#rangeSelect"),
  emphasisSelect: document.querySelector("#emphasisSelect"),
  notice: document.querySelector("#notice"),
  snapshotTitle: document.querySelector("#snapshotTitle"),
  captureMeta: document.querySelector("#captureMeta"),
  captureTotal: document.querySelector("#captureTotal"),
  dominantValue: document.querySelector("#dominantValue"),
  localValue: document.querySelector("#localValue"),
  crossNodeValue: document.querySelector("#crossNodeValue"),
  localProgress: document.querySelector("#localProgress"),
  summaryFootnote: document.querySelector("#summaryFootnote"),
  matrixHead: document.querySelector("#matrixHead"),
  matrixBody: document.querySelector("#matrixBody"),
  matrixRange: document.querySelector("#matrixRange"),
  routeBadge: document.querySelector("#routeBadge"),
  localBarValue: document.querySelector("#localBarValue"),
  localBar: document.querySelector("#localBar"),
  crossBarValue: document.querySelector("#crossBarValue"),
  crossBar: document.querySelector("#crossBar"),
  routeBarValue: document.querySelector("#routeBarValue"),
  routeBar: document.querySelector("#routeBar"),
  fairnessValue: document.querySelector("#fairnessValue"),
  fairnessState: document.querySelector("#fairnessState"),
  fairnessBar: document.querySelector("#fairnessBar"),
  fairnessDetail: document.querySelector("#fairnessDetail"),
  decisionBadge: document.querySelector("#decisionBadge"),
  decisionText: document.querySelector("#decisionText"),
  analyticalValue: document.querySelector("#analyticalValue"),
  observationValue: document.querySelector("#observationValue"),
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

function normalizeWorkload(value) {
  return (value || "UNKNOWN").trim().toUpperCase().replace(/[\s-]+/g, "_");
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
    row.dominant_workload = normalizeWorkload(record.dominant_workload);
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

function displayWorkload(value) {
  return normalizeWorkload(value).replaceAll("_", " ");
}

function getVerdict(recommendation) {
  const key = (recommendation || "REVIEW").split(":", 1)[0].trim();
  return { key, ...(VERDICTS[key] || VERDICTS.REVIEW) };
}

function recommendationDetail(recommendation) {
  const value = recommendation || "";
  const separator = value.indexOf(":");
  if (separator === -1) return value || "No recommendation was reported.";
  const detail = value.slice(separator + 1).trim();
  return detail ? detail.charAt(0).toUpperCase() + detail.slice(1) : "No recommendation was reported.";
}

function setNotice(message = "") {
  elements.notice.hidden = !message;
  elements.notice.textContent = message;
}

function setSource(name, kind, failed = false) {
  state.sourceName = name;
  state.sourceKind = kind;
  elements.sourceName.textContent = name;
  elements.sourceName.title = name;
  elements.sourceKind.textContent = kind;
  elements.sourceFootnote.textContent = `${kind} / ${name}`;
  elements.connectionState.classList.toggle("ready", !failed);
  elements.connectionState.classList.toggle("error", failed);

  if (failed) {
    elements.dataBanner.className = "data-banner error";
    elements.bannerLabel.textContent = "NO WORKLOAD DATA";
    elements.bannerDetail.textContent = "Import a classifier CSV to populate this dashboard.";
    elements.connectionLabel.textContent = "Source unavailable";
  } else if (kind === "Sample source") {
    elements.dataBanner.className = "data-banner sample";
    elements.bannerLabel.textContent = "SAMPLE WORKLOAD DATA";
    elements.bannerDetail.textContent = "Visual preview only. These are not benchmark results.";
    elements.connectionLabel.textContent = "No live connection";
  } else if (kind === "Imported file") {
    elements.dataBanner.className = "data-banner imported";
    elements.bannerLabel.textContent = "IMPORTED WORKLOAD DATA";
    elements.bannerDetail.textContent = "Values are rendered from the selected local CSV.";
    elements.connectionLabel.textContent = "Local CSV loaded";
  } else {
    elements.dataBanner.className = "data-banner export";
    elements.bannerLabel.textContent = "CLASSIFIER EXPORT";
    elements.bannerDetail.textContent = "Showing the latest pg_stat_statements workload captures.";
    elements.connectionLabel.textContent = "CSV export loaded";
  }
}

function getSelectedRow() {
  return state.rows[state.selectedIndex] || null;
}

function getVisibleRows() {
  const selected = getSelectedRow();
  if (!selected) return [];

  const candidates = state.rows.slice(0, state.selectedIndex + 1);
  if (state.range === "all") return candidates;

  const duration = state.range === "24h" ? 24 * 60 * 60 * 1000 : 7 * 24 * 60 * 60 * 1000;
  const threshold = selected.capturedAt.getTime() - duration;
  const filtered = candidates.filter((row) => row.capturedAt.getTime() >= threshold);
  return filtered.length > 0 ? filtered : [selected];
}

function populateCaptureSelect() {
  elements.captureSelect.replaceChildren();
  for (let index = state.rows.length - 1; index >= 0; index -= 1) {
    const option = document.createElement("option");
    option.value = String(index);
    option.textContent = `${index === state.rows.length - 1 ? "Latest - " : ""}${formatCapture(state.rows[index].capturedAt)}`;
    elements.captureSelect.append(option);
  }
  elements.captureSelect.value = String(state.selectedIndex);
}

function renderSummary(latest, visibleRows) {
  const verdict = getVerdict(latest.scale_recommendation);
  elements.snapshotTitle.textContent = `${displayWorkload(latest.dominant_workload)} is the dominant workload`;
  elements.captureMeta.textContent = `${formatCapture(latest.capturedAt)} snapshot, execution-weighted from pg_stat_statements.`;
  elements.captureTotal.textContent = `${visibleRows.length} / ${state.rows.length}`;
  elements.dominantValue.textContent = displayWorkload(latest.dominant_workload);
  elements.localValue.textContent = formatPercent(latest.ideal_local_exec_pct, 0);
  elements.crossNodeValue.textContent = formatPercent(latest.cross_node_exec_pct, 0);
  elements.localProgress.style.width = latest.ideal_local_exec_pct === null
    ? "0%"
    : `${Math.max(0, Math.min(100, latest.ideal_local_exec_pct))}%`;
  elements.summaryFootnote.textContent = `${formatPercent(latest.ideal_local_exec_pct)} ideal-local / fairness ${formatScore(latest.data_distribution_fairness)} / ${verdict.label}`;
}

function appendText(parent, tagName, text, className = "") {
  const element = document.createElement(tagName);
  if (className) element.className = className;
  element.textContent = text;
  parent.append(element);
  return element;
}

function renderMatrix(visibleRows) {
  const captures = visibleRows.slice(-4);
  const selected = getSelectedRow();
  elements.matrixHead.replaceChildren();
  elements.matrixBody.replaceChildren();

  const headerRow = document.createElement("tr");
  const signalHeader = document.createElement("th");
  signalHeader.scope = "col";
  appendText(signalHeader, "strong", "Workload / signal");
  appendText(signalHeader, "small", "Execution-weighted classification");
  headerRow.append(signalHeader);

  captures.forEach((capture, index) => {
    const header = document.createElement("th");
    header.scope = "col";
    if (capture === selected) header.classList.add("selected-capture");
    appendText(header, "span", `Capture ${Math.max(1, visibleRows.length - captures.length + index + 1)}`);
    appendText(header, "strong", formatCapture(capture.capturedAt));
    appendText(header, "small", `${displayWorkload(capture.dominant_workload)} dominant`);
    headerRow.append(header);
  });
  elements.matrixHead.append(headerRow);

  SERIES.forEach((series) => {
    const row = document.createElement("tr");
    row.className = "signal-row";
    if (state.emphasis !== "all" && state.emphasis !== series.focusKey) row.classList.add("is-muted");

    const rowHeader = document.createElement("th");
    rowHeader.scope = "row";
    appendText(rowHeader, "strong", series.label);
    appendText(rowHeader, "span", series.detail);
    row.append(rowHeader);

    captures.forEach((capture) => {
      const value = capture[series.key];
      const captureIndex = state.rows.indexOf(capture);
      const priorValue = captureIndex > 0 ? state.rows[captureIndex - 1][series.key] : null;
      const delta = value === null || priorValue === null ? null : value - priorValue;
      const dominant = capture.dominant_workload === series.dominantKey;
      const cell = document.createElement("td");
      cell.className = `matrix-cell ${value === null ? "missing-cell" : series.cellClass}`;
      if (dominant) cell.classList.add("dominant-cell");

      if (dominant) appendText(cell, "span", "Dominant", "cell-watermark");
      appendText(cell, "strong", formatPercent(value));
      appendText(cell, "span", "workload share");
      const deltaText = delta === null
        ? "No prior capture"
        : Math.abs(delta) < 0.05
          ? "No change vs prior"
          : `${delta > 0 ? "+" : ""}${delta.toFixed(1)} pts vs prior`;
      appendText(cell, "small", deltaText);
      appendText(cell, "em", dominant ? "Dominant profile" : "Profile share", "cell-tag");
      row.append(cell);
    });

    elements.matrixBody.append(row);
  });

  if (captures.length > 0) {
    elements.matrixRange.textContent = `${formatCapture(captures[0].capturedAt)} to ${formatCapture(captures[captures.length - 1].capturedAt)}`;
  } else {
    elements.matrixRange.textContent = "No captures in range";
  }
}

function setMeter(label, bar, value, maximum, percent) {
  label.textContent = percent ? formatPercent(value) : formatScore(value);
  bar.style.width = value === null
    ? "0%"
    : `${Math.max(0, Math.min(100, (value / maximum) * 100))}%`;
}

function renderDiagnostics(latest, visibleRows) {
  setMeter(elements.localBarValue, elements.localBar, latest.ideal_local_exec_pct, 100, true);
  setMeter(elements.crossBarValue, elements.crossBar, latest.cross_node_exec_pct, 100, true);
  setMeter(elements.routeBarValue, elements.routeBar, latest.mean_routability_score, 1, false);

  const routeTone = latest.ideal_local_exec_pct === null
    ? { label: "Not reported", tone: "neutral" }
    : latest.ideal_local_exec_pct >= 70
      ? { label: "Strong", tone: "positive" }
      : latest.ideal_local_exec_pct >= 50
        ? { label: "Watch", tone: "warning" }
        : { label: "Needs attention", tone: "critical" };
  elements.routeBadge.textContent = routeTone.label;
  elements.routeBadge.className = `status-badge ${routeTone.tone}`;

  const fairness = latest.data_distribution_fairness;
  elements.fairnessValue.textContent = formatScore(fairness);
  elements.fairnessBar.style.width = fairness === null
    ? "0%"
    : `${Math.max(0, Math.min(100, fairness * 100))}%`;
  if (fairness === null) {
    elements.fairnessState.textContent = "Not reported";
    elements.fairnessDetail.textContent = "Citus shard metrics are absent from this capture.";
  } else if (fairness >= 0.85) {
    elements.fairnessState.textContent = "Balanced";
    elements.fairnessDetail.textContent = "Shard bytes are distributed evenly enough for scale-out.";
  } else {
    elements.fairnessState.textContent = "Skew detected";
    elements.fairnessDetail.textContent = "Rebalance shard bytes before adding worker capacity.";
  }

  const verdict = getVerdict(latest.scale_recommendation);
  elements.decisionBadge.textContent = verdict.label;
  elements.decisionBadge.className = `decision-badge ${verdict.tone}`;
  elements.decisionText.textContent = recommendationDetail(latest.scale_recommendation);
  elements.analyticalValue.textContent = formatPercent((latest.olap_pct || 0) + (latest.htap_pct || 0));
  elements.observationValue.textContent = visibleRows.length.toLocaleString();
}

function appendCell(row, value, className = "") {
  const cell = document.createElement("td");
  if (value instanceof Node) {
    cell.append(value);
  } else if (className) {
    appendText(cell, "span", value, className);
  } else {
    cell.textContent = value;
  }
  row.append(cell);
}

function selectCapture(index) {
  state.selectedIndex = index;
  elements.captureSelect.value = String(index);
  render();
  document.querySelector(".summary-panel").scrollIntoView({ behavior: "smooth", block: "start" });
}

function renderHistory(visibleRows) {
  elements.historyBody.replaceChildren();
  const captures = visibleRows.slice(-8).reverse();

  captures.forEach((capture) => {
    const captureIndex = state.rows.indexOf(capture);
    const row = document.createElement("tr");
    if (captureIndex === state.selectedIndex) row.classList.add("selected-row");
    const captureButton = document.createElement("button");
    captureButton.type = "button";
    captureButton.className = "capture-link";
    captureButton.textContent = formatCapture(capture.capturedAt);
    captureButton.addEventListener("click", () => selectCapture(captureIndex));
    const workloadClass = `${capture.dominant_workload.toLowerCase().replaceAll("_", "")}tag`;
    const verdict = getVerdict(capture.scale_recommendation);

    appendCell(row, captureButton);
    appendCell(row, displayWorkload(capture.dominant_workload), `workload-tag ${workloadClass}`);
    appendCell(row, formatPercent(capture.oltp_pct));
    appendCell(row, formatPercent(capture.olap_pct));
    appendCell(row, formatPercent(capture.htap_pct));
    appendCell(row, formatPercent(capture.timeseries_pct));
    appendCell(row, formatPercent(capture.ideal_local_exec_pct));
    appendCell(row, verdict.label, "table-decision");
    elements.historyBody.append(row);
  });

  if (visibleRows.length > 0) {
    elements.historyRange.textContent = `${visibleRows.length} captures / ${formatCapture(visibleRows[0].capturedAt)} to ${formatCapture(visibleRows[visibleRows.length - 1].capturedAt)}`;
  } else {
    elements.historyRange.textContent = "No captures in range";
  }
}

function render() {
  const latest = getSelectedRow();
  if (!latest) return;
  const visibleRows = getVisibleRows();
  renderSummary(latest, visibleRows);
  renderMatrix(visibleRows);
  renderDiagnostics(latest, visibleRows);
  renderHistory(visibleRows);
}

function loadText(text, sourceName, sourceKind) {
  const records = parseCSV(text);
  state.rows = normalizeRows(records);
  state.selectedIndex = state.rows.length - 1;
  setSource(sourceName, sourceKind);
  setNotice();
  populateCaptureSelect();
  render();
}

async function loadDefaultSource() {
  elements.refreshButton.disabled = true;
  elements.connectionLabel.textContent = "Locating source";
  elements.connectionState.classList.remove("ready", "error");

  try {
    const response = await fetch("../workload_score.csv", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    loadText(await response.text(), "workload_score.csv", "Classifier export");
  } catch (sourceError) {
    try {
      const response = await fetch("sample-workload.csv", { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      loadText(await response.text(), "sample-workload.csv", "Sample source");
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

elements.importButton.addEventListener("click", () => elements.fileInput.click());
elements.fileInput.addEventListener("change", () => loadFile(elements.fileInput.files[0]));
elements.refreshButton.addEventListener("click", loadDefaultSource);
elements.captureSelect.addEventListener("change", () => {
  state.selectedIndex = Number(elements.captureSelect.value);
  render();
});
elements.rangeSelect.addEventListener("change", () => {
  state.range = elements.rangeSelect.value;
  render();
});
elements.emphasisSelect.addEventListener("change", () => {
  state.emphasis = elements.emphasisSelect.value;
  renderMatrix(getVisibleRows());
});

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