/**
 * Normalizes an OpenAI-compatible base URL.
 *
 * Before:
 * - "https://skybridge-api.com/"
 * - "https://api.example.com/openai/v1"
 *
 * After:
 * - "https://skybridge-api.com/v1/"
 * - "https://api.example.com/openai/v1/"
 */
export function normalizeOpenAICompatibleBaseUrl(value: string | URL | null | undefined): string {
  const raw = value instanceof URL ? value.toString() : typeof value === 'string' ? value.trim() : ''
  if (!raw)
    return ''

  try {
    const url = new URL(raw)
    const pathnameWithoutTrailingSlash = url.pathname.replace(/\/+$/, '')

    if (!pathnameWithoutTrailingSlash) {
      url.pathname = '/v1/'
      return url.toString()
    }

    url.pathname = `${pathnameWithoutTrailingSlash}/`
    return url.toString()
  }
  catch {
    return raw.endsWith('/') ? raw : `${raw}/`
  }
}
