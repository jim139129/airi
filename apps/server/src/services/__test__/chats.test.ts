import { beforeAll, describe, it } from 'vitest'
import { mockDB } from '../../libs/mock-db'
import { createChatService } from '../chats'
import * as schema from '../../schemas/chats'

describe('chatService', () => {
  let db: any
  let service: ReturnType<typeof createChatService>

  beforeAll(async () => {
    db = await mockDB(schema)
    service = createChatService(db)
  })

  it('should sync chat members efficiently (n=500)', async () => {
    // Generate many members to test N+1 query performance
    const numMembers = 500
    const members = Array.from({ length: numMembers }).map((_, i) => ({
      type: 'character' as const,
      characterId: `char-${i}`,
    }))

    const start = performance.now()

    await service.syncChat('user-1', {
      chat: { id: 'chat-2', type: 'group' },
      members,
      messages: [],
    })

    const duration = performance.now() - start
    console.log(`Initial sync (insert) took ${duration.toFixed(2)}ms for ${numMembers} members`)

    const start2 = performance.now()

    await service.syncChat('user-1', {
      chat: { id: 'chat-2', type: 'group' },
      members,
      messages: [],
    })

    const duration2 = performance.now() - start2
    console.log(`Subsequent sync (update) took ${duration2.toFixed(2)}ms for ${numMembers} members`)
  })
})
