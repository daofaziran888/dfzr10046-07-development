<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <form v-if="isHandover" class="handover-check" data-testid="handover-verify" @submit.prevent="runVerify">
      <label class="filter-item">
        <span>凭迟到记录核实</span>
        <input v-model="verifyRecord" data-testid="verify-record" placeholder="如 Q1" />
      </label>
      <label class="filter-item">
        <span>凭时刻核实班次</span>
        <input v-model="verifyAt" data-testid="verify-at" placeholder="如 2026-11-26T09:00:00" />
      </label>
      <button class="btn" data-testid="verify-submit" type="submit">核实交班</button>
      <p v-if="verifyResult" class="handover-verify-result" data-testid="verify-result">
        <template v-if="verifyResult.ok">
          <strong>{{ verifyResult.record_id ? '按记录' : '按时刻' }}可核实：</strong>
          {{ verifyResult.message
            ? verifyResult.message
            : `交班 ${verifyResult.交班时刻 ?? verifyResult.at} · 交班负责人 ${verifyResult.交班负责人 ?? verifyResult.负责人} · 签收时间 ${verifyResult.签收时间 || '未签收'}` }}
        </template>
        <template v-else>{{ verifyResult.message }}</template>
      </p>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="startAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}<em v-if="isRequired(field)" class="required-mark">必填</em></span>
          <input
            v-model="createValues[field]"
            :data-testid="`create-${field}`"
            :required="isRequired(field)"
          />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <div v-if="pendingAction" class="modal-backdrop" data-testid="action-modal">
      <form class="modal" @submit.prevent="confirmAction">
        <h3>{{ pendingAction.action }} · {{ pendingAction.row[meta.fields[0]] ?? pendingAction.row.id }}</h3>
        <label v-for="field in actionContextFields" :key="field" class="create-field">
          <span>{{ field === 'actor' ? '经手人' : '改派给（下一位当班人）' }}</span>
          <input
            v-model="actionContext[field]"
            :data-testid="`action-${field}`"
            :placeholder="field === 'actor' ? '如 甲 / 乙 / 丙' : '如 丙'"
          />
        </label>
        <label class="create-field">
          <span>备注（写入签收结论）</span>
          <input v-model="actionContext.remark" data-testid="action-remark" placeholder="可留空" />
        </label>
        <p v-if="actionHint" class="action-hint">{{ actionHint }}</p>
        <div class="page-actions">
          <button class="btn" type="button" @click="pendingAction = null">取消</button>
          <button class="btn primary" data-testid="confirm-action" type="submit" :disabled="acting">
            {{ acting ? '提交中…' : '确认执行' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  moduleMeta,
  runAction as applyAction,
  verifyHandover,
  type HandoverCheck,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const isHandover = meta.key === 'shift_handover'
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const pendingAction = ref<{ action: string; row: EntryRow } | null>(null)
const actionContext = ref<{ actor: string; target: string; remark: string }>({ actor: '', target: '', remark: '' })
const acting = ref(false)
const verifyRecord = ref('')
const verifyAt = ref('')
const verifyResult = ref<(HandoverCheck & { record_id?: string; message?: string }) | null>(null)

const actionContextFields = computed<('actor' | 'target')[]>(() => {
  if (!pendingAction.value) return []
  return meta.actionFields?.[pendingAction.value.action] ?? []
})
const actionHint = computed(() => {
  const current = pendingAction.value
  if (!current) return ''
  if (current.action === '改派') {
    return '引用：拒收或撤回后的交接单可改派给下一位当班人，改派后重新送交、等待新接班人签收。'
  }
  if (current.action === '签收') {
    return '引用：签收人须为接班负责人，签收后结论写入关联重跑待办并取得排期权限。'
  }
  if (current.action === '撤回') {
    return '引用：撤回人为交班负责人；并发签收与撤回同时到达时只有先落库的一次生效。'
  }
  if (current.action === '拒收') {
    return '引用：拒收由接班负责人发起，退回后可改派下一位当班人排期。'
  }
  if (current.action === '排期重跑' && String(current.row['来源交接单'] ?? '')) {
    return `引用：该任务由交接单 ${current.row['来源交接单']} 交入，须由该交接签收的接班人凭交接排期。`
  }
  return ''
})

function isRequired(field: string) {
  return (meta.requiredFields ?? meta.fields).includes(field)
}

const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  {
    label: meta.metrics[1],
    value: rows.value.filter((row) => row.status === (meta.metricStatuses?.[0] ?? meta.statuses[1])).length,
  },
  {
    label: meta.metrics[2],
    value: rows.value.filter(
      (row) => row.status === (meta.metricStatuses?.[1] ?? meta.statuses[meta.statuses.length - 1]),
    ).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

function startAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  const contextFields = meta.actionFields?.[action] ?? []
  if (!contextFields.length) {
    void runAction(action, row)
    return
  }
  actionContext.value = {
    actor: String(row['接班负责人'] ?? row['负责人'] ?? ''),
    target: String(row['接班负责人'] ?? ''),
    remark: '',
  }
  pendingAction.value = { action, row }
}

async function confirmAction() {
  if (!pendingAction.value) return
  const { action, row } = pendingAction.value
  acting.value = true
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action, {
      actor: actionContext.value.actor.trim() || undefined,
      target: actionContext.value.target.trim() || undefined,
      remark: actionContext.value.remark.trim() || undefined,
    })
    if (!result.ok) throw new Error(result.message)
    pendingAction.value = null
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  } finally {
    acting.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function runVerify() {
  errorMessage.value = ''
  try {
    if (!verifyRecord.value.trim() && !verifyAt.value.trim()) {
      throw new Error('请填写迟到记录或核实时刻')
    }
    const payload = await verifyHandover(meta.key, {
      record_id: verifyRecord.value.trim() || undefined,
      at: verifyAt.value.trim() || undefined,
    })
    verifyResult.value = {
      ...payload,
      record_id: verifyRecord.value.trim() || undefined,
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交班核实失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
