import { describe, expect, it, vi } from 'vitest'

import { providerOpenAICompatible } from './index'

const { createOpenAIMock } = vi.hoisted(() => ({
  createOpenAIMock: vi.fn(() => ({ chat: vi.fn() })),
}))

vi.mock('@xsai-ext/providers/create', () => ({
  createOpenAI: createOpenAIMock,
}))

describe('providerOpenAICompatible', () => {
  it('normalizes root gateway URLs before creating the chat provider', () => {
    providerOpenAICompatible.createProvider({
      apiKey: 'test-key',
      baseUrl: 'https://skybridge-api.com/',
    })

    expect(createOpenAIMock).toHaveBeenCalledWith('test-key', 'https://skybridge-api.com/v1/')
  })
})
