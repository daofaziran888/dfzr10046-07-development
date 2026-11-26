import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
  context: { actor?: string; target?: string; remark?: string } = {},
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action, ...context }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type HandoverCheck = {
  ok: boolean
  message?: string
  交班时刻?: string
  接班时刻?: string
  交班负责人?: string
  接班负责人?: string
  签收时间?: string
  可核实?: boolean
  at?: string
  负责人?: string
  tickets?: Record<string, unknown>[]
  ticket?: Record<string, unknown>
}

export async function verifyHandover(
  key: string,
  query: { record_id?: string; at?: string },
): Promise<HandoverCheck> {
  const params = new URLSearchParams()
  if (query.record_id) params.set('record_id', query.record_id)
  if (query.at) params.set('at', query.at)
  const response = await request(`/api/${key}/verify?${params.toString()}`)
  if (!response.ok) throw new Error('交班核实读取失败')
  return await response.json()
}
