import assert from "node:assert/strict";
import test from "node:test";
import { createJobSchema } from "../src/validation.js";

test("accepts each supported job type and applies defaults", () => {
  const text = createJobSchema.parse({ type: "text_analysis", text: "hello" });
  assert.deepEqual({ cpu: text.cpu, memory: text.memoryMb, gpu: text.gpu, priority: text.priority }, {
    cpu: 1, memory: 256, gpu: 0, priority: 0,
  });
  assert.equal(createJobSchema.parse({ type: "simulated_compute" }).type, "simulated_compute");
  assert.equal(createJobSchema.parse({ type: "always_fail" }).type, "always_fail");
});

test("rejects unsupported types, missing text, invalid resources, priority, and extra fields", () => {
  const invalid = [
    { type: "unknown" },
    { type: "text_analysis" },
    { type: "simulated_compute", cpu: 0 },
    { type: "simulated_compute", memoryMb: -1 },
    { type: "simulated_compute", gpu: -1 },
    { type: "simulated_compute", priority: 11 },
    { type: "simulated_compute", unexpected: true },
  ];
  for (const payload of invalid) assert.equal(createJobSchema.safeParse(payload).success, false);
});
