import { z } from "zod";

export const JOB_TYPES = ["text_analysis", "simulated_compute", "always_fail"] as const;

export const createJobSchema = z.object({
  type: z.enum(JOB_TYPES),
  text: z.string().trim().min(1).max(100_000).optional(),
  cpu: z.number().int().positive().max(128).default(1),
  memoryMb: z.number().int().positive().max(1_048_576).default(256),
  gpu: z.number().int().nonnegative().max(64).default(0),
  priority: z.number().int().min(0).max(10).default(0),
}).strict().superRefine((value, context) => {
  if (value.type === "text_analysis" && !value.text) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ["text"],
      message: "text is required for text_analysis jobs",
    });
  }
});

export type CreateJobInput = z.infer<typeof createJobSchema>;
