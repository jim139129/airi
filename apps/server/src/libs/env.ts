import type { InferOutput } from 'valibot'

import { env, exit } from 'node:process'

import { useLogger } from '@guiiai/logg'
import { injeca } from 'injeca'
import { nonEmpty, object, optional, parse, pipe, string, transform } from 'valibot'

const EnvSchema = object({
  API_SERVER_URL: optional(string(), 'http://localhost:3000'),

  DATABASE_URL: pipe(string(), nonEmpty('DATABASE_URL is required')),

  AUTH_GOOGLE_CLIENT_ID: pipe(string(), nonEmpty('AUTH_GOOGLE_CLIENT_ID is required')),
  AUTH_GOOGLE_CLIENT_SECRET: pipe(string(), nonEmpty('AUTH_GOOGLE_CLIENT_SECRET is required')),
  AUTH_GITHUB_CLIENT_ID: pipe(string(), nonEmpty('AUTH_GITHUB_CLIENT_ID is required')),
  AUTH_GITHUB_CLIENT_SECRET: pipe(string(), nonEmpty('AUTH_GITHUB_CLIENT_SECRET is required')),

  STRIPE_SECRET_KEY: optional(string()),
  STRIPE_WEBHOOK_SECRET: optional(string()),
  CLIENT_URL: pipe(string(), nonEmpty('CLIENT_URL is required')),

  FLUX_PER_CENT: optional(pipe(string(), transform(Number)), '1'),
  FLUX_PER_REQUEST: optional(pipe(string(), transform(Number)), '1'),

  BACKEND_LLM_API_KEY: pipe(string(), nonEmpty('BACKEND_LLM_API_KEY is required')),
  BACKEND_LLM_BASE_URL: pipe(string(), nonEmpty('BACKEND_LLM_BASE_URL is required')),

  // OpenTelemetry
  OTEL_SERVICE_NAMESPACE: optional(string(), 'airi'),
  OTEL_SERVICE_NAME: optional(string(), 'server'),
  OTEL_TRACES_SAMPLING_RATIO: optional(string(), '1.0'),
  OTEL_EXPORTER_OTLP_ENDPOINT: optional(string()),
  OTEL_EXPORTER_OTLP_HEADERS: optional(string()),
  OTEL_DEBUG: optional(string()),
})

export type Env = InferOutput<typeof EnvSchema>

export function parseEnv(inputEnv: Record<string, string> | typeof env): Env {
  try {
    return parse(EnvSchema, inputEnv)
  }
  catch (err) {
    useLogger().withError(err).error('Invalid environment variables')
    exit(1)
  }
}

export const parsedEnv = injeca.provide('env', () => parseEnv(env))
