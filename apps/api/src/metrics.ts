const counters = new Map<string, number>();

export function incrementMetric(name: string, amount = 1) {
  counters.set(name, (counters.get(name) ?? 0) + amount);
}

export function counterValue(name: string) {
  return counters.get(name) ?? 0;
}

export function renderCounters() {
  return [...counters.entries()]
    .map(([name, value]) => `queueflow_${name}_total ${value}`)
    .join("\n");
}
