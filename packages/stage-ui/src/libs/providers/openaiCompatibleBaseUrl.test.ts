import { describe, expect, it } from 'vitest'

import { normalizeOpenAICompatibleBaseUrl } from './openaiCompatibleBaseUrl'

describe('normalizeOpenAICompatibleBaseUrl', () => {
  it('adds /v1/ when users provide only the gateway origin', () => {
    expect(normalizeOpenAICompatibleBaseUrl('https://skybridge-api.com/'))
      .toBe('https://skybridge-api.com/v1/')
    expect(normalizeOpenAICompatibleBaseUrl('https://api.pie-xian.com'))
      .toBe('https://api.pie-xian.com/v1/')
  })

  it('preserves explicit OpenAI-compatible paths and normalizes the trailing slash', () => {
    expect(normalizeOpenAICompatibleBaseUrl('https://api.example.com/openai/v1'))
      .toBe('https://api.example.com/openai/v1/')
    expect(normalizeOpenAICompatibleBaseUrl('https://api.example.com/proxy/'))
      .toBe('https://api.example.com/proxy/')
  })

  it('returns an empty string for missing input', () => {
    expect(normalizeOpenAICompatibleBaseUrl('')).toBe('')
    expect(normalizeOpenAICompatibleBaseUrl(undefined)).toBe('')
  })
})
