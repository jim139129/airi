import type { Action } from '../../libs/mineflayer/action'
import type { Mineflayer } from '../../libs/mineflayer/core'
import type { ActionInstruction } from '../action/types'
import type { BotEvent } from '../types'
import type { PatternRuntime } from './patterns/types'

import { inspect } from 'node:util'

import ivm from 'isolated-vm'

import { createQueryRuntime } from './query-dsl'

interface JavaScriptPlannerOptions {
  timeoutMs?: number
  maxActionsPerTurn?: number
}

interface ActionRuntimeResult {
  action: ActionInstruction
  ok: boolean
  result?: unknown
  error?: string
}

interface ActivePlannerRun {
  actionCount: number
  actionsByName: Map<string, Action>
  executeAction: (action: ActionInstruction) => Promise<unknown>
  executed: ActionRuntimeResult[]
  logs: string[]
  sawSkip: boolean
}

interface ValidationResult {
  action?: ActionInstruction
  error?: string
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isCoord(value: unknown): value is { x: number, y: number, z: number } {
  return isRecord(value)
    && typeof value.x === 'number'
    && typeof value.y === 'number'
    && typeof value.z === 'number'
}

function deepFreeze<T>(value: T): T {
  if (!value || typeof value !== 'object')
    return value

  for (const key of Object.keys(value as Record<string, unknown>)) {
    const child = (value as Record<string, unknown>)[key]
    deepFreeze(child)
  }

  return Object.freeze(value)
}

function toStructuredClone<T>(value: T): T {
  if (value === undefined)
    return undefined as any
  return JSON.parse(JSON.stringify(value)) as T
}

export interface RuntimeGlobals {
  event: BotEvent
  snapshot: Record<string, unknown>
  patterns?: PatternRuntime | null
  mineflayer?: Mineflayer | null
  bot?: unknown
  actionQueue?: unknown
  noActionBudget?: unknown
  errorBurstGuard?: unknown
  currentInput?: unknown
  llmLog?: unknown
  setNoActionBudget?: (value: number) => { ok: true, remaining: number, default: number, max: number }
  getNoActionBudget?: () => { remaining: number, default: number, max: number }
  forgetConversation?: () => { ok: true, cleared: string[] }
  enterContext?: (label: string) => { ok: true, label: string, turnId: number }
  exitContext?: (summary?: string) => { ok: true, summarized: string, messagesArchived: number }
  history?: unknown
  llmInput?: {
    systemPrompt: string
    userMessage: string
    messages: unknown[]
    conversationHistory: unknown[]
    updatedAt: number
    attempt: number
  } | null
}

export interface JavaScriptRunResult {
  actions: ActionRuntimeResult[]
  logs: string[]
  returnValue?: string
}

export interface PlannerGlobalDescriptor {
  name: string
  kind: 'tool' | 'function' | 'object' | 'number' | 'string' | 'boolean' | 'undefined' | 'null' | 'unknown'
  readonly: boolean
  preview: string
}

interface DescribeGlobalsOptions {
  includeBuiltins?: boolean
}

export function extractJavaScriptCandidate(input: string): string {
  const trimmed = input.trim()
  const fenced = trimmed.match(/^```(?:js|javascript|ts|typescript)?[^\S\r\n]*\r?\n?([\s\S]*?)\r?\n?```$/i)
  if (fenced?.[1])
    return fenced[1].trim()

  return trimmed
}

export class JavaScriptPlanner {
  private activeRun: ActivePlannerRun | null = null
  private readonly maxActionsPerTurn: number
  private readonly timeoutMs: number

  private isolate: ivm.Isolate
  private context: ivm.Context
  private globalRef: ivm.Reference<any>
  private mem: Record<string, unknown> = {}
  private lastRun: any = null
  private lastAction: any = null

  constructor(options: JavaScriptPlannerOptions = {}) {
    this.timeoutMs = options.timeoutMs ?? 750
    this.maxActionsPerTurn = options.maxActionsPerTurn ?? 5

    this.isolate = new ivm.Isolate({ memoryLimit: 128 })
    this.context = this.isolate.createContextSync()
    this.globalRef = this.context.global

    this.installBuiltins()
  }

  public async evaluate(
    content: string,
    availableActions: Action[],
    globals: RuntimeGlobals,
    executeAction: (action: ActionInstruction) => Promise<unknown>,
  ): Promise<JavaScriptRunResult> {
    const scriptStr = extractJavaScriptCandidate(content)
    const run: ActivePlannerRun = {
      actionCount: 0,
      actionsByName: new Map(availableActions.map(action => [action.name, action])),
      executeAction,
      executed: [],
      logs: [],
      sawSkip: false,
    }

    this.activeRun = run
    this.installActionTools(availableActions)
    this.bindRuntimeGlobals(globals, run)

    try {
      let result

      try {
        if (scriptStr.includes('await ')) {
          const wrapped = `(async () => {\n${scriptStr}\n})()`
          const script = await this.isolate.compileScript(wrapped)
          result = await script.run(this.context, { timeout: this.timeoutMs, promise: true, copy: true })
        }
        else {
          const script = await this.isolate.compileScript(scriptStr)
          result = await script.run(this.context, { timeout: this.timeoutMs, copy: true })
          if (result instanceof Promise) {
            result = await result
          }
        }
      }
      catch (e: any) {
        if (e.message && e.message.includes('Illegal return statement')) {
          const wrapped = `(async () => {\n${scriptStr}\n})()`
          const script = await this.isolate.compileScript(wrapped)
          result = await script.run(this.context, { timeout: this.timeoutMs, promise: true, copy: true })
        }
        else {
          throw e
        }
      }

      const returnValue = typeof result === 'undefined'
        ? undefined
        : inspect(result, {
            depth: null,
            breakLength: 100,
            maxArrayLength: 100,
            maxStringLength: 10_000,
          })

      const memRef = this.globalRef.getSync('mem')
      if (memRef && memRef.copySync) {
        this.mem = memRef.copySync()
      }

      this.lastRun = {
        actions: run.executed,
        logs: run.logs,
        returnRaw: result,
      }
      this.globalRef.setSync('lastRun', new ivm.ExternalCopy(this.lastRun).copyInto())

      return {
        actions: run.executed,
        logs: run.logs,
        returnValue,
      }
    }
    catch (err: any) {
      if (err && typeof err === 'object') {
        if (err.message) {
          if (err.message.includes('Script execution timed out')) {
            throw new Error('Script execution timed out.')
          }
          if (err.message.includes('An object was thrown from supplied code')) {
            throw new Error(err.message)
          }
          else {
            throw new Error(err.message)
          }
        }
      }

      throw new Error(String(err.message || err))
    }
    finally {
      if (this.activeRun && (this.activeRun as any).trackedRefs) {
        for (const ref of (this.activeRun as any).trackedRefs) {
          try { ref.release() }
          catch (e) { }
        }
      }
      this.activeRun = null
      this.clearActionTools(availableActions)

      try {
        const llmLogRef = this.globalRef.getSync('$llmLog')
        if (llmLogRef instanceof ivm.Reference) {
          llmLogRef.release()
        }
      }
      catch (e) { }

      // Release query function references
      if (globals.mineflayer) {
        try {
          const queryKeys = Object.keys(createQueryRuntime(globals.mineflayer))
          for (const k of queryKeys) {
            const r = this.globalRef.getSync(`$query_${k}`)
            if (r instanceof ivm.Reference)
              r.release()
            this.globalRef.deleteSync(`$query_${k}`)
          }
        }
        catch (e) { }
      }
    }
  }

  public canEvaluateAsExpression(content: string): boolean {
    const scriptStr = extractJavaScriptCandidate(content)
    if (!scriptStr.trim())
      return false

    try {
      this.isolate.compileScriptSync(`(async () => (\n${scriptStr}\n))()`)
      return true
    }
    catch {
      return false
    }
  }

  public describeGlobals(
    availableActions: Action[],
    globals: RuntimeGlobals,
    options: DescribeGlobalsOptions = {},
  ): PlannerGlobalDescriptor[] {
    const descriptors: PlannerGlobalDescriptor[] = []

    const includeBuiltins = options.includeBuiltins ?? true

    const staticGlobals: Array<Omit<PlannerGlobalDescriptor, 'preview'>> = [
      { name: 'skip', kind: 'tool', readonly: true },
      { name: 'use', kind: 'function', readonly: true },
      { name: 'log', kind: 'function', readonly: true },
      { name: 'expect', kind: 'function', readonly: true },
      { name: 'expectMoved', kind: 'function', readonly: true },
      { name: 'expectNear', kind: 'function', readonly: true },
      { name: 'snapshot', kind: 'object', readonly: true },
      { name: 'event', kind: 'object', readonly: true },
      { name: 'now', kind: 'number', readonly: true },
      { name: 'self', kind: 'object', readonly: true },
      { name: 'environment', kind: 'object', readonly: true },
      { name: 'social', kind: 'object', readonly: true },
      { name: 'threat', kind: 'object', readonly: true },
      { name: 'attention', kind: 'object', readonly: true },
      { name: 'autonomy', kind: 'object', readonly: true },
      { name: 'llmInput', kind: 'object', readonly: true },
      { name: 'currentInput', kind: 'object', readonly: true },
      { name: 'llmLog', kind: 'object', readonly: true },
      { name: 'actionQueue', kind: 'object', readonly: true },
      { name: 'noActionBudget', kind: 'object', readonly: true },
      { name: 'errorBurstGuard', kind: 'object', readonly: true },
      { name: 'setNoActionBudget', kind: 'function', readonly: true },
      { name: 'getNoActionBudget', kind: 'function', readonly: true },
      { name: 'forget_conversation', kind: 'function', readonly: true },
      { name: 'enterContext', kind: 'function', readonly: true },
      { name: 'exitContext', kind: 'function', readonly: true },
      { name: 'history', kind: 'object', readonly: true },
      { name: 'llmMessages', kind: 'object', readonly: true },
      { name: 'llmSystemPrompt', kind: 'string', readonly: true },
      { name: 'llmUserMessage', kind: 'string', readonly: true },
      { name: 'llmConversationHistory', kind: 'object', readonly: true },
      { name: 'query', kind: 'object', readonly: true },
      { name: 'query.self', kind: 'function', readonly: true },
      { name: 'query.snapshot', kind: 'function', readonly: true },
      { name: 'query.gaze', kind: 'function', readonly: true },
      { name: 'patterns', kind: 'object', readonly: true },
      { name: 'patterns.get', kind: 'function', readonly: true },
      { name: 'patterns.find', kind: 'function', readonly: true },
      { name: 'patterns.ids', kind: 'function', readonly: true },
      { name: 'patterns.list', kind: 'function', readonly: true },
      { name: 'bot', kind: 'object', readonly: true },
      { name: 'mineflayer', kind: 'object', readonly: true },
      { name: 'mem', kind: 'object', readonly: false },
      { name: 'lastRun', kind: 'object', readonly: true },
      { name: 'prevRun', kind: 'object', readonly: true },
      { name: 'lastAction', kind: 'object', readonly: true },
    ]

    const valueByName: Record<string, unknown> = {
      'snapshot': globals.snapshot,
      'event': globals.event,
      'now': Date.now(),
      'self': (globals.snapshot as Record<string, unknown>)?.self,
      'environment': (globals.snapshot as Record<string, unknown>)?.environment,
      'social': (globals.snapshot as Record<string, unknown>)?.social,
      'threat': (globals.snapshot as Record<string, unknown>)?.threat,
      'attention': (globals.snapshot as Record<string, unknown>)?.attention,
      'autonomy': (globals.snapshot as Record<string, unknown>)?.autonomy,
      'llmInput': globals.llmInput ?? null,
      'currentInput': globals.currentInput ?? null,
      'llmLog': globals.llmLog ?? null,
      'actionQueue': globals.actionQueue ?? null,
      'noActionBudget': globals.noActionBudget ?? null,
      'errorBurstGuard': globals.errorBurstGuard ?? null,
      'llmMessages': globals.llmInput?.messages ?? [],
      'llmSystemPrompt': globals.llmInput?.systemPrompt ?? '',
      'llmUserMessage': globals.llmInput?.userMessage ?? '',
      'llmConversationHistory': globals.llmInput?.conversationHistory ?? [],
      'query': globals.mineflayer ? createQueryRuntime(globals.mineflayer) : undefined,
      'patterns': globals.patterns ?? null,
      'patterns.get': globals.patterns?.get,
      'patterns.find': globals.patterns?.find,
      'patterns.ids': globals.patterns?.ids,
      'patterns.list': globals.patterns?.list,
      'bot': globals.bot ?? globals.mineflayer?.bot,
      'mineflayer': globals.mineflayer ?? null,
      'mem': this.mem,
      'lastRun': this.lastRun,
      'prevRun': this.lastRun ?? null,
      'lastAction': this.lastAction,
      'skip': () => { },
      'use': () => { },
      'log': () => { },
      'expect': () => { },
      'expectMoved': () => { },
      'expectNear': () => { },
      'setNoActionBudget': globals.setNoActionBudget,
      'getNoActionBudget': globals.getNoActionBudget,
      'forget_conversation': globals.forgetConversation,
      'enterContext': globals.enterContext,
      'exitContext': globals.exitContext,
      'history': globals.history,
    }

    if (includeBuiltins) {
      for (const item of staticGlobals) {
        descriptors.push({
          ...item,
          preview: this.previewValue(valueByName[item.name]),
        })
      }
    }

    for (const action of availableActions) {
      descriptors.push({
        name: action.name,
        kind: 'tool',
        readonly: true,
        preview: action.description || '(tool)',
      })
    }

    descriptors.sort((a, b) => a.name.localeCompare(b.name))
    return descriptors
  }

  private installBuiltins(): void {
    this.context.evalSync(`
      globalThis.console = {
        log: () => {},
        error: () => {},
        warn: () => {}
      };
    `)

    this.defineGlobalTool('skip', async () => this.runAction('skip', {}))
    this.defineGlobalTool('use', async (toolName: unknown, params?: unknown) => {
      if (typeof toolName !== 'string' || toolName.length === 0) {
        throw new Error('use(toolName, params) requires a non-empty string toolName')
      }

      const mappedParams = isRecord(params) ? params : {}
      return this.runAction(toolName, mappedParams)
    })
    this.defineGlobalTool('log', (...args: unknown[]) => {
      if (!this.activeRun)
        throw new Error('log() is only allowed during REPL evaluation')

      const rendered = args.map(arg => inspect(arg, { depth: 4, breakLength: 120 })).join(' ')
      this.activeRun.logs.push(rendered)
      return rendered
    })
    this.defineGlobalTool('expect', (condition: unknown, message?: unknown) => {
      if (condition)
        return true

      const detail = typeof message === 'string' && message.trim().length > 0
        ? message
        : 'Condition evaluated to false'
      throw new Error(`Expectation failed: ${detail}`)
    })
    this.defineGlobalTool('expectMoved', (minBlocks?: unknown, message?: unknown) => {
      const threshold = typeof minBlocks === 'number' ? minBlocks : 0.5
      const telemetry = this.getLastActionResultRecord()
      const movedDistance = typeof telemetry?.movedDistance === 'number'
        ? telemetry.movedDistance
        : null

      if (movedDistance === null) {
        throw new Error('Expectation failed: expectMoved() requires last action result with movedDistance telemetry')
      }

      if (movedDistance >= threshold)
        return true

      const detail = typeof message === 'string' && message.trim().length > 0
        ? message
        : `Expected movedDistance >= ${threshold}, got ${movedDistance}`
      throw new Error(`Expectation failed: ${detail}`)
    })
    this.defineGlobalTool('expectNear', (targetOrMaxDist?: unknown, maxDistOrMessage?: unknown, maybeMessage?: unknown) => {
      const telemetry = this.getLastActionResultRecord()

      let target: { x: number, y: number, z: number } | null = null
      let maxDist = 2
      let message: string | undefined

      if (isCoord(targetOrMaxDist)) {
        target = { x: targetOrMaxDist.x, y: targetOrMaxDist.y, z: targetOrMaxDist.z }
        if (typeof maxDistOrMessage === 'number')
          maxDist = maxDistOrMessage
        if (typeof maybeMessage === 'string')
          message = maybeMessage
      }
      else {
        if (typeof targetOrMaxDist === 'number')
          maxDist = targetOrMaxDist
        if (typeof maxDistOrMessage === 'string')
          message = maxDistOrMessage
      }

      let distance: number | null = null
      if (target) {
        const endPos = isCoord(telemetry?.endPos) ? telemetry.endPos : null
        if (!endPos) {
          throw new Error('Expectation failed: expectNear(target) requires last action result with endPos telemetry')
        }

        const dx = endPos.x - target.x
        const dy = endPos.y - target.y
        const dz = endPos.z - target.z
        distance = Math.sqrt(dx * dx + dy * dy + dz * dz)
      }
      else if (typeof telemetry?.distanceToTargetAfter === 'number') {
        distance = telemetry.distanceToTargetAfter
      }

      if (distance === null) {
        throw new Error('Expectation failed: expectNear() requires target argument or last action distanceToTargetAfter telemetry')
      }

      if (distance <= maxDist)
        return true

      const detail = message ?? `Expected distance <= ${maxDist}, got ${distance}`
      throw new Error(`Expectation failed: ${detail}`)
    })

    this.context.evalSync(`globalThis.mem = {};`)
  }

  private getLastActionResultRecord(): Record<string, unknown> | null {
    if (!isRecord(this.lastAction))
      return null

    const result = this.lastAction.result
    return isRecord(result) ? result : null
  }

  private installActionTools(availableActions: Action[]): void {
    for (const action of availableActions) {
      const toolRef = new ivm.Reference(async (...args: unknown[]) => {
        const params = this.mapArgsToParams(action, args)
        const res = await this.runAction(action.name, params)
        return new ivm.ExternalCopy(res).copyInto()
      })
      this.globalRef.setSync(`$${action.name}`, toolRef)

      this.context.evalSync(`
          globalThis["${action.name}"] = async (...args) => {
              const ref = globalThis['$${action.name}'];
              return ref.apply(undefined, args, { arguments: { copy: true }, result: { promise: true, copy: true } });
          }
      `)
    }
  }

  private clearActionTools(availableActions: Action[]): void {
    for (const action of availableActions) {
      this.context.evalSync(`delete globalThis["${action.name}"];`)
      try {
        const ref = this.globalRef.getSync(`$${action.name}`)
        if (ref instanceof ivm.Reference) {
          ref.release()
        }
      }
      catch (e) { }
      this.globalRef.deleteSync(`$${action.name}`)
    }
  }

  private bindRuntimeGlobals(globals: RuntimeGlobals, run: ActivePlannerRun): void {
    const snapshot = toStructuredClone(globals.snapshot)
    const event = toStructuredClone(globals.event)
    const llmInput = toStructuredClone(globals.llmInput ?? null)
    const currentInput = toStructuredClone(globals.currentInput ?? null)
    const actionQueue = toStructuredClone(globals.actionQueue ?? null)
    const noActionBudget = toStructuredClone(globals.noActionBudget ?? null)
    const errorBurstGuard = toStructuredClone(globals.errorBurstGuard ?? null)

    const setSync = (key: string, val: any) => {
      if (val === undefined) {
        this.globalRef.deleteSync(key)
      }
      else {
        this.globalRef.setSync(key, new ivm.ExternalCopy(val).copyInto())
      }
    }

    setSync('prevRun', this.lastRun ?? null)
    setSync('snapshot', snapshot)
    setSync('event', event)
    setSync('now', Date.now())
    setSync('self', snapshot.self)
    setSync('environment', snapshot.environment)
    setSync('social', snapshot.social)
    setSync('threat', snapshot.threat)
    setSync('attention', snapshot.attention)
    setSync('autonomy', snapshot.autonomy)
    setSync('llmInput', llmInput)
    setSync('currentInput', currentInput)

    if (globals.llmLog && typeof globals.llmLog === 'object' && typeof (globals.llmLog as any).query === 'function') {
      this.globalRef.setSync('$llmLog', new ivm.Reference((...args: any[]) => {
        try {
          const res = (globals.llmLog as any).query(...args)
          return new ivm.ExternalCopy(res ? res.list() : []).copyInto()
        }
        catch {
          return undefined
        }
      }))
      this.context.evalSync(`
        globalThis.llmLog = {
          query: (...args) => {
            return globalThis['$llmLog'].applySync(undefined, args, { arguments: { copy: true }, result: { copy: true } });
          }
        };
      `)
    }
    else if (typeof globals.llmLog === 'function') {
      const llmLogRef = new ivm.Reference((...args: any[]) => {
        try {
          const res = (globals.llmLog as Function)(...args)
          return new ivm.ExternalCopy(res ? res.list() : []).copyInto()
        }
        catch {
          return undefined
        }
      })
      this.globalRef.setSync('$llmLog', llmLogRef)
      this.context.evalSync(`
        globalThis.llmLog = (...args) => {
          return globalThis['$llmLog'].applySync(undefined, args, { arguments: { copy: true }, result: { copy: true } });
        }
      `)
    }
    else {
      setSync('llmLog', globals.llmLog ?? null)
    }

    setSync('actionQueue', actionQueue)
    setSync('noActionBudget', noActionBudget)
    setSync('errorBurstGuard', errorBurstGuard)
    setSync('llmMessages', llmInput?.messages ?? [])
    setSync('llmSystemPrompt', llmInput?.systemPrompt ?? '')
    setSync('llmUserMessage', llmInput?.userMessage ?? '')
    setSync('llmConversationHistory', llmInput?.conversationHistory ?? [])

    const trackedRefs: ivm.Reference<any>[] = []

    // Expose query object functions
    if (globals.mineflayer) {
      const query = createQueryRuntime(globals.mineflayer) as any
      const boundQuery: Record<string, unknown> = {}
      for (const [key, val] of Object.entries(query)) {
        if (typeof val === 'function') {
          const ref = new ivm.Reference((...args: any[]) => {
            try {
              const res = (val as Function)(...args)
              if (res && typeof res.list === 'function') {
                return new ivm.ExternalCopy(res.list()).copyInto()
              }
              return new ivm.ExternalCopy(res).copyInto()
            }
            catch (e) {
              return undefined
            }
          })
          trackedRefs.push(ref)
          this.globalRef.setSync(`$query_${key}`, ref)
          boundQuery[key] = true
        }
      }

      this.context.evalSync(`
        globalThis.query = {
          ${Object.keys(boundQuery).map(k => `${k}: (...args) => globalThis['$query_${k}'].applySync(undefined, args, { arguments: { copy: true }, result: { copy: true } })`).join(',\n          ')}
        };
      `)
    }
    else {
      this.globalRef.deleteSync('query')
      this.context.evalSync(`delete globalThis.query;`)
    }

    if (globals.bot) {
      setSync('bot', { username: (globals.bot as any).username })
    }
    if (globals.mineflayer) {
      setSync('mineflayer', { version: (globals.mineflayer as any).bot?.version })
    }

    const bindFunc = (name: string, fn: Function | null | undefined) => {
      if (fn) {
        const ref = new ivm.Reference((...args: any[]) => {
          const res = fn(...args)
          return res !== undefined ? new ivm.ExternalCopy(res).copyInto() : undefined
        })
        trackedRefs.push(ref)
        this.globalRef.setSync(`$${name}`, ref)
        this.context.evalSync(`
            globalThis["${name}"] = (...args) => {
                const r = globalThis['$${name}'];
                return r.applySync(undefined, args, { arguments: { copy: true }, result: { copy: true } });
            }
        `)
      }
      else {
        this.globalRef.deleteSync(`$${name}`)
        this.context.evalSync(`delete globalThis["${name}"];`)
      }
    }

    bindFunc('setNoActionBudget', globals.setNoActionBudget)
    bindFunc('getNoActionBudget', globals.getNoActionBudget)
    bindFunc('forget_conversation', globals.forgetConversation)
    bindFunc('enterContext', globals.enterContext)
    bindFunc('exitContext', globals.exitContext)

    if (globals.patterns) {
      const patternsRef = {
        get: new ivm.Reference((...args: any[]) => new ivm.ExternalCopy(globals.patterns!.get!(...args)).copyInto()),
        find: new ivm.Reference((...args: any[]) => new ivm.ExternalCopy(globals.patterns!.find!(...args)).copyInto()),
        ids: new ivm.Reference((...args: any[]) => new ivm.ExternalCopy(globals.patterns!.ids!(...args)).copyInto()),
        list: new ivm.Reference((...args: any[]) => new ivm.ExternalCopy(globals.patterns!.list!(...args)).copyInto()),
      }
      trackedRefs.push(patternsRef.get, patternsRef.find, patternsRef.ids, patternsRef.list)
      this.globalRef.setSync('$patternsGet', patternsRef.get)
      this.globalRef.setSync('$patternsFind', patternsRef.find)
      this.globalRef.setSync('$patternsIds', patternsRef.ids)
      this.globalRef.setSync('$patternsList', patternsRef.list)
      this.context.evalSync(`
        globalThis.patterns = {
          get: (...args) => $patternsGet.applySync(undefined, args, { result: { copy: true }, arguments: { copy: true } }),
          find: (...args) => $patternsFind.applySync(undefined, args, { result: { copy: true }, arguments: { copy: true } }),
          ids: (...args) => $patternsIds.applySync(undefined, args, { result: { copy: true }, arguments: { copy: true } }),
          list: (...args) => $patternsList.applySync(undefined, args, { result: { copy: true }, arguments: { copy: true } })
        };
      `)
    }
    else {
      this.globalRef.deleteSync('patterns')
    }

    setSync('lastRun', {
      actions: run.executed,
      logs: run.logs,
      returnRaw: undefined,
    })
    setSync('lastAction', this.lastAction)

    this.globalRef.setSync('mem', new ivm.ExternalCopy(this.mem).copyInto())

    ; (run as any).trackedRefs = trackedRefs
  }

  private mapArgsToParams(action: Action, args: unknown[]): Record<string, unknown> {
    const shape = action.schema.shape as Record<string, unknown>
    const keys = Object.keys(shape)

    if (keys.length === 0)
      return {}

    if (args.length === 1) {
      const [firstArg] = args
      if (isRecord(firstArg))
        return firstArg

      if (keys.length === 1)
        return { [keys[0]]: firstArg }
    }

    const params: Record<string, unknown> = {}
    for (const [index, key] of keys.entries()) {
      if (index >= args.length)
        break
      params[key] = args[index]
    }

    return params
  }

  private async runAction(tool: string, params: Record<string, unknown>): Promise<ActionRuntimeResult> {
    if (!this.activeRun) {
      throw new Error('Tool calls are only allowed during REPL evaluation')
    }

    if (this.activeRun.sawSkip && tool !== 'skip') {
      throw new Error('skip() cannot be mixed with other tool calls in the same script')
    }

    if (this.activeRun.actionCount >= this.maxActionsPerTurn) {
      throw new Error(`Action limit exceeded: max ${this.maxActionsPerTurn} actions per turn`)
    }

    if (tool === 'skip') {
      this.activeRun.sawSkip = true
    }

    this.activeRun.actionCount++

    if (tool === 'skip') {
      const action: ActionInstruction = { tool: 'skip', params: {} }
      const runtimeResult: ActionRuntimeResult = {
        action,
        ok: true,
        result: 'Skipped turn',
      }
      this.activeRun.executed.push(runtimeResult)
      this.lastAction = runtimeResult
      this.globalRef.setSync('lastAction', new ivm.ExternalCopy(this.lastAction).copyInto())
      return runtimeResult
    }

    const validation = this.validateAction(tool, params)
    if (!validation.action) {
      const runtimeResult: ActionRuntimeResult = {
        action: { tool, params },
        ok: false,
        error: validation.error ?? `Invalid tool parameters for ${tool}`,
      }
      this.activeRun.executed.push(runtimeResult)
      this.lastAction = runtimeResult
      this.globalRef.setSync('lastAction', new ivm.ExternalCopy(this.lastAction).copyInto())
      return runtimeResult
    }
    const action = validation.action

    try {
      const result = await this.activeRun.executeAction(action)
      const runtimeResult: ActionRuntimeResult = {
        action,
        ok: true,
        result,
      }
      this.activeRun.executed.push(runtimeResult)
      this.lastAction = runtimeResult
      this.globalRef.setSync('lastAction', new ivm.ExternalCopy(this.lastAction).copyInto())
      return runtimeResult
    }
    catch (error) {
      const runtimeResult: ActionRuntimeResult = {
        action,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      }
      this.activeRun.executed.push(runtimeResult)
      this.lastAction = runtimeResult
      this.globalRef.setSync('lastAction', new ivm.ExternalCopy(this.lastAction).copyInto())
      return runtimeResult
    }
  }

  private validateAction(tool: string, params: Record<string, unknown>): ValidationResult {
    if (!this.activeRun)
      throw new Error('Tool calls are only allowed during REPL evaluation')

    const action = this.activeRun.actionsByName.get(tool)
    if (!action)
      throw new Error(`Unknown tool: ${tool}`)

    const parsed = action.schema.safeParse(params)
    if (!parsed.success) {
      const details = parsed.error.issues
        .map(issue => `${issue.path.map(item => String(item)).join('.') || 'root'}: ${issue.message}`)
        .join('; ')
      return {
        error: `Invalid tool parameters for ${tool}: ${details}`,
      }
    }

    return { action: { tool, params: parsed.data } }
  }

  private defineGlobalTool(name: string, fn: (...args: any[]) => any): void {
    const isAsync = fn.constructor.name === 'AsyncFunction'
    if (isAsync) {
      this.globalRef.setSync(`$${name}`, new ivm.Reference(async (...args: any[]) => {
        try {
          const res = await fn(...args)
          return res !== undefined ? new ivm.ExternalCopy(res).copyInto() : undefined
        }
        catch (err: any) {
          return new ivm.ExternalCopy({ __error: err instanceof Error ? err.message : String(err) }).copyInto()
        }
      }))
      this.context.evalSync(`
          globalThis["${name}"] = async (...args) => {
              const ref = globalThis['$${name}'];
              try {
                const res = await ref.apply(undefined, args, { arguments: { copy: true }, result: { promise: true, copy: true } });
                if (res && typeof res === 'object' && res.__error) {
                  throw new Error(res.__error);
                }
                return res;
              } catch (err) {
                if (err && typeof err === 'object' && err.message) {
                  throw new Error(err.message);
                }
                throw new Error(String(err));
              }
          }
      `)
    }
    else {
      this.globalRef.setSync(`$${name}`, new ivm.Reference((...args: any[]) => {
        try {
          const res = fn(...args)
          return res !== undefined ? new ivm.ExternalCopy(res).copyInto() : undefined
        }
        catch (err: any) {
          return new ivm.ExternalCopy({ __error: err instanceof Error ? err.message : String(err) }).copyInto()
        }
      }))
      this.context.evalSync(`
          globalThis["${name}"] = (...args) => {
              const ref = globalThis['$${name}'];
              const res = ref.applySync(undefined, args, { arguments: { copy: true }, result: { copy: true } });
              if (res && typeof res === 'object' && res.__error) {
                throw new Error(res.__error);
              }
              return res;
          }
      `)
    }
  }

  private previewValue(value: unknown): string {
    if (value === null)
      return 'null'
    if (typeof value === 'undefined')
      return 'undefined'
    if (typeof value === 'string')
      return value.length > 120 ? `${value.slice(0, 117)}...` : value

    const rendered = inspect(value, { depth: 1, breakLength: 120 })
    return rendered.length > 120 ? `${rendered.slice(0, 117)}...` : rendered
  }
}
