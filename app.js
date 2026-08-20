// 入湖流水线控制台 - 真实运行版前端逻辑
// 所有任务、映射和运行状态均来自本机 Python 后端，不再使用 mock 数据。

(function () {
  'use strict';

  // ============ 全局状态 ============
  var state = {
    // resource.text 中当前待执行的任务。
    tasks: [],
    // 后端解析出的真实逐表运行结果。
    results: [],
    // 已持久化的历史运行摘要。
    history: [],
    // 正式资源目录中的三份配置。
    mapping: {
      system: '',
      script: '',
      clob: ''
    },
    // 当前页面视图。
    view: 'config',
    // 打开抽屉前的焦点元素，关闭后把焦点还给原来的操作入口。
    drawerReturnFocus: null,
    // 是否存在正在运行或停止中的真实流水线。
    running: false,
    // 当前真实流水线是否正在停止。
    stopping: false,
    // 当前真实流水线运行 ID。
    runId: null,
    // 后端状态轮询定时器。
    pollTimer: null,
    // 避免重复弹出同一次运行的完成提示。
    completedRunId: null,
    // 记录当前页面刚提交的运行，防止终态轮询把已提交任务重新填回输入框。
    submittedRunId: null,
    // 目录筛选与已选表分开保存，切换筛选条件时不清空已选表。
    catalog: {
      systems: [],
      available: false,
      loading: false,
      status: '等待连接',
      systemId: '',
      sourceIds: [],
      databases: {},
      items: [],
      selectedTables: [],
      unrecognized: [],
      errors: [],
      searching: false
    },
    // 数据源管理只保存后端公开字段，密码仅在表单提交时读取，绝不进入状态。
    sourceManagement: {
      systems: [],
      sources: [],
      selectedSystemId: '',
      loading: false,
      loaded: false,
      validatingIds: [],
      feedback: null,
      error: null
    },
    // SeaTunnel 物理任务管理：状态、延迟与只读配置查看。
    seatunnel: {
      jobs: [],
      nodes: [],
      nodeSummary: {},
      nodesCheckedAt: null,
      nodeError: null,
      loading: false,
      loaded: false,
      pendingActions: [],
      pollTimer: null,
      polling: false,
      pollDeadline: 0,
      // 记录每个任务最近一次启停操作的日志，供「查看日志」按钮回看。
      actionLogs: {},
      // 实时日志轮询定时器。
      operationPollTimer: null,
      // 当前日志弹窗正在查看的操作 ID，用于轮询时更新可见内容。
      viewingOperationId: null
    },
    // 数据重跑：直接操作选定环境中的 StarRocks ODS 表。
    rerun: {
      environments: [],
      selectedEnvironments: ['test', 'prod'],
      available: false,
      loading: false,
      status: '等待连接',
      items: [],
      errors: [],
      searching: false,
      loaded: false,
      running: false,
      operationId: null,
      operationStatus: null,
      operationPollTimer: null,
      viewingOperationId: null,
      completed: 0,
      total: 0,
      history: [],
      historyLoading: false,
      historyQuery: '',
      // key -> { environment, environmentLabel, table, oriExists }
      selected: {}
    }
  };

  // ============ 常量 ============
  var API_BASE = '/api';
  var POLL_INTERVAL_MS = 1200;
  // SeaTunnel 任务启停后自动刷新列表，跟踪远程引擎状态变化。
  // 每次刷新会读取状态库并 SSH 查询引擎，间隔不宜过短。
  var SEATUNNEL_POLL_INTERVAL_MS = 5000;
  var SEATUNNEL_POLL_MAX_MS = 180000;

  // 页面展示阶段，与 run_pipeline.py 的阶段保持一致。
  var STAGES = [
    { key: 'check', label: '前置校验' },
    { key: 'test', label: '测试环境初始化' },
    { key: 'stop', label: '停止 SeaTunnel' },
    { key: 'prod', label: '生产环境初始化' },
    { key: 'conf', label: '生成并上传 Conf' },
    { key: 'start', label: '启动 SeaTunnel' }
  ];

  // ============ 通用工具 ============
  function $(selector) {
    return document.querySelector(selector);
  }

  // 对动态文本进行 HTML 转义，避免表名或日志内容进入 innerHTML 时破坏页面。
  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // 使用页面内轻提示承载成功、失败和校验结果，避免频繁弹出阻塞式对话框。
  function notify(message, type, duration) {
    var container = $('#toast-container');
    if (!container) return;

    var toast = document.createElement('div');
    toast.className = 'toast' + (type ? ' is-' + type : '');
    toast.setAttribute('role', type === 'error' ? 'alert' : 'status');
    toast.textContent = String(message || '');
    container.appendChild(toast);

    window.setTimeout(function () {
      if (toast.parentNode) toast.parentNode.removeChild(toast);
    }, duration || (type === 'error' ? 6000 : 3600));
  }

  // 统一请求后端 JSON 接口，并把后端错误转换成可读异常。
  function requestJson(url, options) {
    return fetch(url, options).then(function (response) {
      return response.json().catch(function () {
        return { error: '后端返回了无法解析的响应' };
      }).then(function (payload) {
        if (!response.ok) {
          throw new Error(payload.error || ('请求失败，HTTP ' + response.status));
        }
        return payload;
      });
    });
  }

  // 从任务文本解析结构化数据，用于浏览器端即时格式校验。
  function parseTaskLine(line) {
    var text = (line || '').trim();
    if (!text) return null;

    var parts = text.split(/\s+/);
    if (parts.length < 3) {
      return {
        raw: text,
        valid: false,
        reason: '字段不足，应为「别名 源表名 操作描述」'
      };
    }

    var alias = parts[0];
    var table = parts[1];
    var description = parts.slice(2).join(' ');
    var operation;

    // “字段”的判断必须早于“表”，避免描述中同时出现时被错误识别为建表。
    if (description.indexOf('字段') !== -1) {
      operation = 'add_field';
    } else if (description.indexOf('表') !== -1) {
      operation = 'new_table';
    } else {
      return {
        raw: text,
        valid: false,
        reason: '操作描述需包含「表」或「字段」'
      };
    }

    return {
      raw: text,
      valid: true,
      alias: alias,
      table: table,
      description: description,
      operation: operation,
      opLabel: operation === 'new_table' ? '新建表' : '新增字段'
    };
  }

  function getClobWhitelistErrors(text) {
    var errors = [];
    String(text || '').split(/\r?\n/).forEach(function (rawLine, index) {
      var line = rawLine.trim();
      if (!line || line.indexOf('#') === 0) return;
      var parts = line.split('.').map(function (part) {
        return part.trim();
      });
      if (parts.length !== 3 || parts.some(function (part) { return !part; })) {
        errors.push('第 ' + (index + 1) + ' 行应为：系统.Schema.表名');
      }
    });
    return errors;
  }

  function readTasksFromInput() {
    return $('#task-input').value
      .split(/\r?\n/)
      .map(parseTaskLine)
      .filter(Boolean);
  }

  function getValidTasksOrAlert() {
    var parsed = readTasksFromInput();
    var invalid = parsed.filter(function (task) {
      return !task.valid;
    });

    if (!parsed.length) {
      notify('任务列表不能为空。', 'warning');
      return null;
    }
    if (invalid.length) {
      notify(
        '发现 ' + invalid.length + ' 条格式错误：\n' +
        invalid.map(function (task) {
          return '- ' + task.raw + '（' + task.reason + '）';
        }).join('\n'),
        'error',
        8000
      );
      return null;
    }
    return parsed;
  }

  // ============ 后端接口 ============
  var api = {
    health: function () {
      return requestJson(API_BASE + '/health');
    },

    fetchTasks: function () {
      return requestJson(API_BASE + '/tasks');
    },

    fetchCatalog: function () {
      return requestJson(API_BASE + '/catalog');
    },

    fetchSourceManagement: function () {
      return requestJson(API_BASE + '/source-management');
    },

    createSourceSystem: function (payload) {
      return requestJson(API_BASE + '/source-management/systems', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
    },

    deleteSourceSystem: function (systemId) {
      return requestJson(API_BASE + '/source-management/systems/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ systemId: systemId })
      });
    },

    createSource: function (payload) {
      return requestJson(API_BASE + '/source-management/sources', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
    },

    updateSource: function (payload) {
      return requestJson(API_BASE + '/source-management/sources/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
    },

    deleteSource: function (sourceId) {
      return requestJson(API_BASE + '/source-management/sources/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sourceId: sourceId,
          confirmSourceId: sourceId
        })
      });
    },

    validateSource: function (sourceId) {
      return requestJson(API_BASE + '/source-management/sources/validate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sourceId: sourceId })
      });
    },

    searchCatalogTables: function (targets, query) {
      return requestJson(API_BASE + '/catalog/tables', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          targets: targets,
          query: query,
          limit: 300
        })
      });
    },

    saveTasks: function (text) {
      return requestJson(API_BASE + '/tasks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: text })
      });
    },

    fetchMapping: function () {
      return requestJson(API_BASE + '/mapping');
    },

    saveMapping: function (mapping) {
      return requestJson(API_BASE + '/mapping', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(mapping)
      });
    },

    // confirmed=true 是后端强制要求的生产操作确认标识。
    runPipeline: function (text) {
      return requestJson(API_BASE + '/pipeline/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          text: text,
          confirmed: true
        })
      });
    },

    fetchPipelineStatus: function () {
      return requestJson(API_BASE + '/pipeline/status');
    },

    stopPipeline: function (runId) {
      return requestJson(API_BASE + '/pipeline/stop', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          runId: runId,
          confirmed: true
        })
      });
    },

    fetchPipelineHistory: function () {
      return requestJson(API_BASE + '/pipeline/history');
    },

    fetchPipelineHistoryDetail: function (runId) {
      return requestJson(API_BASE + '/pipeline/history/' + encodeURIComponent(runId));
    },

    fetchSeatunnelJobs: function () {
      return requestJson(API_BASE + '/seatunnel/jobs');
    },

    fetchSeatunnelNodes: function () {
      return requestJson(API_BASE + '/seatunnel/nodes');
    },

    fetchSeatunnelConfig: function (name) {
      return requestJson(API_BASE + '/seatunnel/jobs/' + encodeURIComponent(name) + '/config');
    },

    startSeatunnelJob: function (name) {
      return requestJson(API_BASE + '/seatunnel/jobs/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: name, confirmed: true })
      });
    },

    stopSeatunnelJob: function (name) {
      return requestJson(API_BASE + '/seatunnel/jobs/stop', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: name, confirmed: true })
      });
    },

    restartSeatunnelJob: function (name) {
      return requestJson(API_BASE + '/seatunnel/jobs/restart', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: name, confirmed: true })
      });
    },

    fetchSeatunnelOperationLog: function (operationId) {
      return requestJson(API_BASE + '/seatunnel/logs/' + encodeURIComponent(operationId));
    },

    fetchRerunEnvironments: function () {
      return requestJson(API_BASE + '/rerun/environments');
    },

    searchRerunTables: function (environment, query) {
      return requestJson(API_BASE + '/rerun/tables', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ environment: environment, query: query, limit: 300 })
      });
    },

    runRerun: function (environments, tables, productionConfirmed) {
      return requestJson(API_BASE + '/rerun/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          environments: environments,
          confirmed: true,
          productionConfirmed: productionConfirmed === true,
          tables: tables
        })
      });
    },

    fetchRerunStatus: function (operationId) {
      return requestJson(API_BASE + '/rerun/status/' + encodeURIComponent(operationId));
    },

    fetchRerunHistory: function () {
      return requestJson(API_BASE + '/rerun/history');
    },

    fetchRerunHistoryDetail: function (operationId) {
      return requestJson(API_BASE + '/rerun/history/' + encodeURIComponent(operationId));
    }
  };

  // ============ 目录选择器 ============
  function catalogTableKey(item) {
    return [item.sourceId, item.database, item.table].map(function (value) {
      return String(value || '').toLowerCase();
    }).join('\u0001');
  }

  function splitTableQuery(query) {
    var seen = {};
    return String(query || '').split(/[,，\r\n]+/).map(function (item) {
      return item.trim();
    }).filter(function (item) {
      var key = item.toLowerCase();
      if (!item || seen[key]) return false;
      seen[key] = true;
      return true;
    });
  }

  function getCurrentSystem() {
    return state.catalog.systems.find(function (system) {
      return String(system.id) === state.catalog.systemId;
    }) || null;
  }

  function getCatalogSource(sourceId) {
    var matched = null;
    state.catalog.systems.some(function (system) {
      matched = (system.dataSources || []).find(function (source) {
        return String(source.id) === String(sourceId);
      }) || null;
      return Boolean(matched);
    });
    return matched;
  }


  function addSelectedCatalogTable(item) {
    var key = catalogTableKey(item);
    var exists = state.catalog.selectedTables.some(function (selected) {
      return catalogTableKey(selected) === key;
    });
    if (!exists) {
      state.catalog.selectedTables.push({
        sourceId: item.sourceId,
        database: item.database,
        table: item.table
      });
    }
  }

  function formatCatalogError(error) {
    if (typeof error === 'string') return error;
    if (!error) return '未知目录错误';
    if (error.message) return error.message;
    var location = [error.sourceId, error.database].filter(Boolean).join(' / ');
    return (location ? location + '：' : '') + (error.error || error.reason || JSON.stringify(error));
  }

  function catalogHasTargets() {
    return state.catalog.sourceIds.some(function (sourceId) {
      return (state.catalog.databases[sourceId] || []).length > 0;
    });
  }

  function updateCatalogControls() {
    var selector = $('#catalog-selector');
    if (!selector) return;

    var unavailable = state.running || !state.catalog.available || state.catalog.loading;
    selector.querySelectorAll('.catalog-control').forEach(function (control) {
      control.disabled = unavailable;
    });
    if (unavailable) return;

    selector.querySelectorAll('[name="catalog-system"]').forEach(function (control) {
      control.disabled = false;
    });
    selector.querySelectorAll('[data-catalog-target]').forEach(function (control) {
      control.disabled = !state.catalog.systemId;
    });

    var hasTargets = catalogHasTargets();
    $('#catalog-query').disabled = !hasTargets || state.catalog.searching;
    $('#btn-catalog-search').disabled = !hasTargets || state.catalog.searching || !$('#catalog-query').value.trim();
    selector.querySelectorAll('[data-catalog-result-index]').forEach(function (control) {
      control.disabled = state.catalog.searching;
    });
    selector.querySelectorAll('[name="catalog-operation"]').forEach(function (control) {
      control.disabled = !state.catalog.selectedTables.length;
    });
    selector.querySelectorAll('[data-selected-table-index]').forEach(function (control) {
      control.disabled = false;
    });
    $('#btn-clear-selected-tables').disabled = !state.catalog.selectedTables.length;
    $('#btn-add-selected-tasks').disabled = !state.catalog.selectedTables.length;
  }

  function renderCatalog() {
    var catalog = state.catalog;
    var status = $('#catalog-status');
    status.textContent = catalog.loading ? '目录读取中' : (catalog.available ? '目录已连接' : catalog.status);

    if (catalog.loading) {
      $('#catalog-systems').innerHTML = '<p class="muted">正在读取业务系统…</p>';
    } else if (!catalog.available) {
      $('#catalog-systems').innerHTML = '<p class="muted">' + escapeHtml(catalog.status) + '</p>';
    } else {
      $('#catalog-systems').innerHTML = catalog.systems.map(function (system) {
        var id = String(system.id);
        return '<label class="selector-option">' +
          '<input class="catalog-control" type="radio" name="catalog-system" value="' + escapeHtml(id) + '"' +
            (catalog.systemId === id ? ' checked' : '') + ' />' +
          '<span class="selector-option-text">' + escapeHtml(system.label || id) + '</span>' +
          '</label>';
      }).join('') || '<p class="muted">目录中暂无业务系统</p>';
    }

    var system = getCurrentSystem();
    var sources = system ? (system.dataSources || []) : [];
    $('#catalog-targets').innerHTML = sources.map(function (source) {
      var sourceId = String(source.id);
      var selectedDatabases = catalog.databases[sourceId] || [];
      return (source.databases || []).map(function (database) {
        var databaseName = String(database);
        var sourceLabel = source.label || sourceId;
        return '<label class="selector-option">' +
          '<input class="catalog-control" type="checkbox" data-catalog-target="' + escapeHtml(sourceId) +
            '" value="' + escapeHtml(databaseName) + '"' +
            (selectedDatabases.indexOf(databaseName) !== -1 ? ' checked' : '') + ' />' +
          '<span class="selector-option-text">' + escapeHtml(sourceLabel + ' / ' + databaseName) +
            '<small>' + escapeHtml(sourceId + ' · ' + source.type) + '</small></span>' +
          '</label>';
      }).join('');
    }).join('') || '<p class="muted">' + (system ? '该系统暂无数据源 / 数据库' : '请先选择业务系统') + '</p>'; 

    var feedback = [];
    if (catalog.unrecognized.length) {
      feedback.push('<div class="feedback-warn">未精确识别（可从模糊结果中勾选）：' + escapeHtml(catalog.unrecognized.join('、')) + '</div>');
    }
    if (catalog.errors.length) {
      feedback.push('<div class="feedback-error">后端错误：' + catalog.errors.map(function (error) {
        return escapeHtml(formatCatalogError(error));
      }).join('；') + '</div>');
    }
    $('#catalog-feedback').innerHTML = feedback.join('');

    if (catalog.searching) {
      $('#catalog-results').innerHTML = '<p class="muted">正在搜索表…</p>';
    } else {
      $('#catalog-results').innerHTML = catalog.items.map(function (item, index) {
        var checked = catalog.selectedTables.some(function (selected) {
          return catalogTableKey(selected) === catalogTableKey(item);
        });
        return '<label class="selector-option">' +
          '<input class="catalog-control" type="checkbox" data-catalog-result-index="' + index + '"' +
            (checked ? ' checked' : '') + ' />' +
          '<span class="selector-option-text">' + escapeHtml(item.table) +
            '<small>' + escapeHtml(item.sourceId + ' / ' + item.database) +
              (item.exact ? ' · 精确匹配' : ' · 模糊匹配') + '</small></span>' +
          '</label>';
      }).join('') || '<p class="muted">' + (catalogHasTargets() ? '输入表名后搜索' : '请选择数据源 / 数据库并输入表名') + '</p>';
    }

    $('#selected-table-count').textContent = catalog.selectedTables.length + ' 张';
    $('#selected-table-tags').innerHTML = catalog.selectedTables.map(function (item, index) {
      var label = item.sourceId + ' / ' + item.database + '.' + item.table;
      return '<span class="selected-tag"><span>' + escapeHtml(label) + '</span>' +
        '<button class="catalog-control" type="button" data-selected-table-index="' + index +
          '" aria-label="移除 ' + escapeHtml(label) + '">×</button></span>';
    }).join('') || '<span class="muted">尚未选择表</span>';

    updateCatalogControls();
  }

  // catalog 刷新后移除已经失效的数据源、数据库和已选表，避免提交脏选择。
  function reconcileCatalogSelection() {
    var catalog = state.catalog;
    var sourceMap = {};
    var sourceSystemMap = {};
    var systemIds = {};

    catalog.systems.forEach(function (system) {
      var systemId = String(system.id || '');
      systemIds[systemId] = true;
      (system.dataSources || []).forEach(function (source) {
        var sourceId = String(source.id || '');
        sourceMap[sourceId] = source;
        sourceSystemMap[sourceId] = systemId;
      });
    });

    if (catalog.systemId && !systemIds[catalog.systemId]) {
      catalog.systemId = '';
    }

    catalog.sourceIds = catalog.sourceIds.filter(function (sourceId) {
      return Boolean(sourceMap[sourceId]) && sourceSystemMap[sourceId] === catalog.systemId;
    });

    var databases = {};
    catalog.sourceIds.forEach(function (sourceId) {
      var allowed = (sourceMap[sourceId].databases || []).map(String);
      var selected = (catalog.databases[sourceId] || []).filter(function (database) {
        return allowed.indexOf(String(database)) !== -1;
      });
      if (selected.length) databases[sourceId] = selected;
    });
    catalog.databases = databases;
    catalog.sourceIds = catalog.sourceIds.filter(function (sourceId) {
      return Boolean(catalog.databases[sourceId] && catalog.databases[sourceId].length);
    });

    catalog.selectedTables = catalog.selectedTables.filter(function (item) {
      var source = sourceMap[String(item.sourceId || '')];
      return Boolean(source) && (source.databases || []).map(String).indexOf(String(item.database || '')) !== -1;
    });
    catalog.items = catalog.items.filter(function (item) {
      var source = sourceMap[String(item.sourceId || '')];
      return Boolean(source) && (source.databases || []).map(String).indexOf(String(item.database || '')) !== -1;
    });
  }

  function loadCatalog() {
    state.catalog.loading = true;
    state.catalog.status = '目录读取中';
    renderCatalog();
    return api.fetchCatalog().then(function (payload) {
      state.catalog.systems = Array.isArray(payload.systems) ? payload.systems : [];
      reconcileCatalogSelection();
      state.catalog.available = true;
      state.catalog.status = '目录已连接';
    }).catch(function (error) {
      state.catalog.systems = [];
      reconcileCatalogSelection();
      state.catalog.available = false;
      state.catalog.status = '目录不可用：' + error.message;
    }).then(function () {
      state.catalog.loading = false;
      renderCatalog();
    });
  }

  // ============ 数据源管理 ============
  function getManagedSystem(systemId) {
    return state.sourceManagement.systems.find(function (system) {
      return String(system.id) === String(systemId);
    }) || null;
  }

  function getManagedSource(sourceId) {
    return state.sourceManagement.sources.find(function (source) {
      return String(source.id) === String(sourceId);
    }) || null;
  }

  function getSystemSources(systemId) {
    return state.sourceManagement.sources.filter(function (source) {
      return String(source.systemId) === String(systemId);
    });
  }

  function sourceValidationDisplay(source) {
    if (!source.managed || source.readOnly) {
      return { label: '正式配置', className: 'ok' };
    }
    var map = {
      valid: { label: '有效', className: 'ok' },
      stale: { label: '待重新验证', className: 'warn' },
      invalid: { label: '无效', className: 'danger' }
    };
    return map[source.validationStatus] || { label: source.validationStatus || '待重新验证', className: 'warn' };
  }

  function setSourceManagementMessage(scope, message, isError) {
    var management = state.sourceManagement;
    management.feedback = isError ? null : { scope: scope, message: message };
    management.error = isError ? { scope: scope, message: message } : null;
    renderSourceManagement();
  }

  function renderSourceFeedback(element, scope) {
    var management = state.sourceManagement;
    var item = management.error && management.error.scope === scope
      ? management.error
      : (management.feedback && management.feedback.scope === scope ? management.feedback : null);
    element.classList.toggle('is-error', Boolean(item && management.error === item));
    element.classList.toggle('is-ok', Boolean(item && management.feedback === item));
    element.innerHTML = item ? escapeHtml(item.message) : '';
  }

  function renderSourceManagement() {
    var systemList = $('#source-system-list');
    if (!systemList) return;

    var management = state.sourceManagement;
    var selectedSystem = getManagedSystem(management.selectedSystemId);
    var selectedSources = selectedSystem ? getSystemSources(selectedSystem.id) : [];
    var mutationDisabled = management.loading || state.running;

    $('#btn-add-source-system').disabled = mutationDisabled;
    $('#btn-refresh-sources').disabled = management.loading;
    $('#btn-refresh-sources').textContent = management.loading ? '刷新中…' : '刷新';
    $('#btn-add-source').disabled = mutationDisabled || !selectedSystem;
    renderSourceFeedback($('#source-system-feedback'), 'system');
    renderSourceFeedback($('#source-feedback'), 'source');

    if (management.loading && !management.systems.length) {
      systemList.innerHTML = '<p class="muted">正在加载业务系统…</p>';
    } else {
      systemList.innerHTML = management.systems.map(function (system) {
        var systemId = String(system.id || '');
        var sourceCount = getSystemSources(systemId).length;
        var canDelete = Boolean(system.managed && !system.readOnly && sourceCount === 0);
        return '<div class="source-system-item' +
            (management.selectedSystemId === systemId ? ' is-active' : '') +
            '" data-source-system-id="' + escapeHtml(systemId) + '" role="button" tabindex="0" aria-pressed="' +
            (management.selectedSystemId === systemId ? 'true' : 'false') + '">' +
          '<span class="source-system-name"><strong>' + escapeHtml(system.label || systemId) + '</strong>' +
            '<small>' + escapeHtml(systemId + ' · ' + sourceCount + ' 个数据源') + '</small></span>' +
          (system.readOnly ? '<span class="pill">只读</span>' : '') +
          (canDelete ? '<button class="btn btn-ghost btn-sm" type="button" data-delete-source-system="' +
            escapeHtml(systemId) + '" aria-label="删除业务系统 ' + escapeHtml(system.label || systemId) + '"' +
            (mutationDisabled ? ' disabled' : '') + '>删除</button>' : '') +
          '</div>';
      }).join('') || '<p class="muted">暂无业务系统，可先新增系统。</p>';
    }

    $('#source-list-title').textContent = selectedSystem ? (selectedSystem.label || selectedSystem.id) + ' / 数据源' : '数据源';
    $('#source-list-description').textContent = selectedSystem
      ? (selectedSystem.readOnly ? '正式系统中的现有数据源只读；仍可新增安全托管的数据源。' : '管理该业务系统下的数据源连接。')
      : '请先选择业务系统';
    $('#source-count').textContent = selectedSources.length + ' 个';

    if (management.loading && !management.loaded) {
      $('#source-body').innerHTML = '<tr><td colspan="7" class="muted" style="text-align:center;padding:24px;">正在加载数据源…</td></tr>';
      return;
    }

    $('#source-body').innerHTML = selectedSources.map(function (source) {
      var sourceId = String(source.id || '');
      var validation = sourceValidationDisplay(source);
      var validating = management.validatingIds.indexOf(sourceId) !== -1;
      var managed = Boolean(source.managed && !source.readOnly);
      var connection = managed
        ? escapeHtml((source.host || '-') + ':' + (source.port || '-')) + '<small>' + escapeHtml(source.username || '-') + '</small>'
        : '正式配置<small>连接信息由现有配置维护</small>';
      var checkedAt = source.checkedAt ? ' · ' + formatDateTime(source.checkedAt) : '';
      var pipelineReady = source.pipelineReady !== false;
      var actions = managed
        ? '<div class="source-actions">' +
          '<button class="btn btn-secondary btn-sm" type="button" data-validate-source="' + escapeHtml(sourceId) + '"' +
            (mutationDisabled || validating ? ' disabled' : '') + '>' + (validating ? '验证中…' : '验证') + '</button>' +
          '<button class="btn btn-ghost btn-sm" type="button" data-edit-source="' + escapeHtml(sourceId) + '"' +
            (mutationDisabled || validating ? ' disabled' : '') + '>编辑</button>' +
          '<button class="btn btn-danger btn-sm" type="button" data-delete-source="' + escapeHtml(sourceId) + '"' +
            (mutationDisabled || validating ? ' disabled' : '') + '>删除</button>' +
          '</div>'
        : '<span class="pill">只读</span>';
      return '<tr>' +
        '<td><span class="source-name"><strong>' + escapeHtml(source.label || sourceId) + '</strong>' +
          '<small>' + escapeHtml(sourceId) + '</small></span></td>' +
        '<td>' + escapeHtml(String(source.type || '-').toUpperCase()) + '</td>' +
        '<td><span class="source-name">' + connection + '</span></td>' +
        '<td>' + escapeHtml((source.databases || []).join('、') || '-') + '</td>' +
        '<td><span class="pill ' + validation.className + '">' + escapeHtml(validation.label) + '</span>' +
          '<span class="source-name"><small title="' + escapeHtml(source.message || '') + '">' +
            escapeHtml((source.message || '') + checkedAt) + '</small></span></td>' +
        '<td><span class="pill ' + (pipelineReady ? 'ok' : 'warn') + '">' +
          (pipelineReady ? '已就绪' : '配置未完整') + '</span></td>' +
        '<td>' + actions + '</td>' +
        '</tr>';
    }).join('') || '<tr><td colspan="7" class="muted" style="text-align:center;padding:24px;">' +
      (selectedSystem ? '该系统暂无数据源' : '请先选择业务系统') + '</td></tr>';
  }

  function loadSourceManagement() {
    var management = state.sourceManagement;
    management.loading = true;
    management.error = null;
    renderSourceManagement();
    return api.fetchSourceManagement().then(function (payload) {
      management.systems = Array.isArray(payload.systems) ? payload.systems : [];
      management.sources = Array.isArray(payload.sources) ? payload.sources : [];
      if (!getManagedSystem(management.selectedSystemId)) {
        management.selectedSystemId = management.systems.length ? String(management.systems[0].id) : '';
      }
      management.loaded = true;
      management.loading = false;
      renderSourceManagement();
      return payload;
    }).catch(function (error) {
      management.loading = false;
      management.error = { scope: 'source', message: '加载数据源管理失败：' + error.message };
      renderSourceManagement();
      throw error;
    });
  }

  function refreshAfterSourceMutation() {
    return Promise.all([loadSourceManagement(), loadCatalog()]);
  }

  function performSourceMutation(requestFactory, options) {
    var management = state.sourceManagement;
    management.loading = true;
    management.feedback = null;
    management.error = null;
    renderSourceManagement();
    return requestFactory().then(function (payload) {
      if (options.selectedSystemId !== undefined) {
        management.selectedSystemId = options.selectedSystemId;
      }
      if (options.closeDrawer) closeDrawer();
      return refreshAfterSourceMutation().then(function () {
        setSourceManagementMessage(options.scope, options.successMessage, false);
        return payload;
      });
    }).catch(function (error) {
      management.loading = false;
      setSourceManagementMessage(options.scope, error.message, true);
      throw error;
    });
  }

  function splitSourceList(value) {
    var seen = {};
    return String(value || '').split(/[,，\r\n]+/).map(function (item) {
      return item.trim();
    }).filter(function (item) {
      if (!item || seen[item]) return false;
      seen[item] = true;
      return true;
    });
  }

  function syncOracleSourceFields(form) {
    var isOracle = form.elements.type.value === 'oracle';
    form.querySelectorAll('[data-oracle-field]').forEach(function (field) {
      field.hidden = !isOracle;
      field.querySelectorAll('input, select').forEach(function (control) {
        control.disabled = !isOracle;
        control.required = isOracle && control.name === 'oracleService';
      });
    });
  }

  function openSourceDrawer(source) {
    var editing = Boolean(source);
    var selectedSystemId = editing ? String(source.systemId) : state.sourceManagement.selectedSystemId;
    var systemOptions = state.sourceManagement.systems.map(function (system) {
      var systemId = String(system.id || '');
      return '<option value="' + escapeHtml(systemId) + '"' +
        (systemId === selectedSystemId ? ' selected' : '') + '>' +
        escapeHtml((system.label || systemId) + ' (' + systemId + ')') + '</option>';
    }).join('');
    var type = editing ? String(source.type || 'mysql').toLowerCase() : 'mysql';
    var sourceId = editing ? String(source.id || '') : '';

    $('#drawer').classList.remove('is-history');
    $('#drawer-title').textContent = editing ? '编辑数据源 / ' + sourceId : '新增数据源';
    $('#drawer-body').innerHTML = '<form class="source-form" id="source-form">' +
      '<div class="source-form-grid">' +
        '<div class="source-form-field"><label for="source-form-system">业务系统</label>' +
          '<select id="source-form-system" name="systemId" required>' + systemOptions + '</select></div>' +
        '<div class="source-form-field"><label for="source-form-id">数据源 ID</label>' +
          '<input id="source-form-id" name="id" value="' + escapeHtml(sourceId) +
            '" pattern="[a-z][a-z0-9_]{1,39}" maxlength="40" required' + (editing ? ' readonly' : '') + ' /></div>' +
        '<div class="source-form-field"><label for="source-form-label">显示名称</label>' +
          '<input id="source-form-label" name="label" value="' + escapeHtml(editing ? source.label : '') + '" maxlength="200" required /></div>' +
        '<div class="source-form-field"><label for="source-form-type">数据库类型</label>' +
          '<select id="source-form-type" name="type"><option value="mysql"' + (type === 'mysql' ? ' selected' : '') +
            '>MySQL</option><option value="oracle"' + (type === 'oracle' ? ' selected' : '') + '>Oracle</option></select></div>' +
        '<div class="source-form-field"><label for="source-form-host">主机</label>' +
          '<input id="source-form-host" name="host" value="' + escapeHtml(editing ? source.host : '') + '" maxlength="255" required /></div>' +
        '<div class="source-form-field"><label for="source-form-port">端口</label>' +
          '<input id="source-form-port" name="port" type="number" min="1" max="65535" value="' +
            escapeHtml(editing ? source.port : 3306) + '" required /></div>' +
        '<div class="source-form-field"><label for="source-form-username">用户名</label>' +
          '<input id="source-form-username" name="username" value="' + escapeHtml(editing ? source.username : '') + '" maxlength="200" autocomplete="username" required /></div>' +
        '<div class="source-form-field"><label for="source-form-password">密码' + (editing ? '（留空不修改）' : '') + '</label>' +
          '<input id="source-form-password" name="password" type="password" autocomplete="new-password"' +
            (editing ? '' : ' required') + ' /></div>' +
        '<div class="source-form-field" data-oracle-field><label for="source-form-oracle-mode">Oracle 连接模式</label>' +
          '<select id="source-form-oracle-mode" name="oracleMode"><option value="serviceName"' +
            (!editing || source.oracleMode !== 'sid' ? ' selected' : '') + '>Service Name</option><option value="sid"' +
            (editing && source.oracleMode === 'sid' ? ' selected' : '') + '>SID</option></select></div>' +
        '<div class="source-form-field" data-oracle-field><label for="source-form-oracle-service">Oracle Service / SID</label>' +
          '<input id="source-form-oracle-service" name="oracleService" value="' +
            escapeHtml(editing ? source.oracleService : '') + '" /></div>' +
        '<div class="source-form-field is-wide"><label for="source-form-databases">数据库 / Schema（逗号或换行分隔）</label>' +
          '<textarea id="source-form-databases" name="databases" required>' +
            escapeHtml(editing ? (source.databases || []).join('\n') : '') + '</textarea></div>' +
        '<div class="source-form-field is-wide"><label for="source-form-aliases">别名（逗号或换行分隔）</label>' +
          '<textarea id="source-form-aliases" name="aliases" required>' +
            escapeHtml(editing ? (source.aliases || []).join('\n') : '') + '</textarea></div>' +
        '<div class="source-form-field is-wide"><label for="source-form-test-script">测试初始化脚本</label>' +
          '<input id="source-form-test-script" name="testScript" pattern="[A-Za-z][A-Za-z0-9_]{0,79}" value="' + escapeHtml(editing ? source.testScript : 'TestAutoManaged') + '" />' +
          '<span class="source-form-hint">填写 starrocks/test 下不带 .py 的脚本名，例如 TestAutoBPM；留空表示仅管理连接。</span></div>' +
        '<div class="source-form-field is-wide"><label for="source-form-prod-script">生产初始化脚本</label>' +
          '<input id="source-form-prod-script" name="prodScript" pattern="[A-Za-z][A-Za-z0-9_]{0,79}" value="' + escapeHtml(editing ? source.prodScript : 'AutoManaged') + '" />' +
          '<span class="source-form-hint">填写 starrocks 下不带 .py 的脚本名，例如 AutoManaged。</span></div>' +
        '<div class="source-form-field"><label for="source-form-ods-prefix">ODS 表名前缀</label>' +
          '<input id="source-form-ods-prefix" name="odsPrefix" pattern="ods_[a-z0-9_]{2,80}_" value="' + escapeHtml(editing ? source.odsPrefix : '') + '" placeholder="ods_demo_app_" required /></div>' +
        '<div class="source-form-field"><label for="source-form-resource">StarRocks Resource</label>' +
          '<input id="source-form-resource" name="starrocksResource" pattern="[A-Za-z][A-Za-z0-9_]{1,79}" value="' + escapeHtml(editing ? source.starrocksResource : '') + '" placeholder="pro_demo" required /></div>' +
        '<div class="source-form-field"><label for="source-form-startup-mode">CDC 启动模式</label>' +
          '<select id="source-form-startup-mode" name="startupMode"><option value="latest"' + (!editing || source.startupMode !== 'initial' ? ' selected' : '') + '>latest</option><option value="initial"' + (editing && source.startupMode === 'initial' ? ' selected' : '') + '>initial</option></select></div>' +
        '<div class="source-form-field"><label for="source-form-parallelism">SeaTunnel 并行度</label>' +
          '<input id="source-form-parallelism" name="parallelism" type="number" min="1" max="64" value="' + escapeHtml(editing ? (source.parallelism || 1) : 1) + '" required /></div>' +
        '<div class="source-form-field is-wide"><span class="source-form-hint">标准 managed 物理任务名与数据源 ID 相同；当前每个任务只支持一个数据库或 Schema。</span></div>' +
      '</div>' +
      '<div class="source-feedback" id="source-form-feedback" aria-live="polite"></div>' +
      '<div class="row-actions"><span class="spacer"></span>' +
        '<button class="btn btn-ghost" type="button" data-source-form-cancel>取消</button>' +
        '<button class="btn btn-primary" type="submit">' + (editing ? '保存修改' : '创建数据源') + '</button>' +
      '</div>' +
      '</form>';

    var form = $('#source-form');
    syncOracleSourceFields(form);
    form.elements.type.addEventListener('change', function () {
      syncOracleSourceFields(form);
    });
    form.querySelector('[data-source-form-cancel]').addEventListener('click', closeDrawer);
    form.addEventListener('submit', function (event) {
      event.preventDefault();
      if (!form.reportValidity()) return;

      var databases = splitSourceList(form.elements.databases.value);
      var aliases = splitSourceList(form.elements.aliases.value);
      if (!databases.length || !aliases.length) {
        $('#source-form-feedback').textContent = '数据库 / Schema 和别名均不能为空。';
        $('#source-form-feedback').classList.add('is-error');
        return;
      }

      var payload = {
        id: form.elements.id.value.trim(),
        systemId: form.elements.systemId.value,
        label: form.elements.label.value.trim(),
        type: form.elements.type.value,
        host: form.elements.host.value.trim(),
        port: Number(form.elements.port.value),
        username: form.elements.username.value.trim(),
        databases: databases,
        oracleMode: form.elements.oracleMode.value || 'serviceName',
        oracleService: form.elements.oracleService.value.trim(),
        aliases: aliases,
        testScript: form.elements.testScript.value.trim(),
        prodScript: form.elements.prodScript.value.trim(),
        odsPrefix: form.elements.odsPrefix.value.trim(),
        starrocksResource: form.elements.starrocksResource.value.trim(),
        startupMode: form.elements.startupMode.value,
        parallelism: Number(form.elements.parallelism.value)
      };
      var password = form.elements.password.value;
      if (password) payload.password = password;
      if (editing) payload.sourceId = sourceId;

      var submitButton = form.querySelector('[type="submit"]');
      submitButton.disabled = true;
      submitButton.textContent = editing ? '保存中…' : '创建中…';
      performSourceMutation(function () {
        return editing ? api.updateSource(payload) : api.createSource(payload);
      }, {
        scope: 'source',
        selectedSystemId: payload.systemId,
        successMessage: editing ? '数据源已更新，请重新验证连接。' : '数据源已创建，请验证连接。',
        closeDrawer: true
      }).catch(function (error) {
        if (!document.body.contains(form)) return;
        submitButton.disabled = false;
        submitButton.textContent = editing ? '保存修改' : '创建数据源';
        $('#source-form-feedback').textContent = error.message;
        $('#source-form-feedback').classList.add('is-error');
      });
    });

    showDrawer();
  }

  function validateManagedSource(sourceId) {
    var management = state.sourceManagement;
    if (management.validatingIds.indexOf(sourceId) !== -1) return;
    management.validatingIds.push(sourceId);
    management.feedback = null;
    management.error = null;
    renderSourceManagement();

    api.validateSource(sourceId).then(function (payload) {
      if (payload.source) {
        management.sources = management.sources.map(function (source) {
          return String(source.id) === sourceId ? payload.source : source;
        });
      }
      var updated = payload.source || getManagedSource(sourceId) || {};
      setSourceManagementMessage('source', updated.message || payload.message || '验证已完成', !payload.ok);
      return loadCatalog();
    }).catch(function (error) {
      setSourceManagementMessage('source', error.message, true);
    }).then(function () {
      management.validatingIds = management.validatingIds.filter(function (id) {
        return id !== sourceId;
      });
      renderSourceManagement();
    });
  }

  function searchCatalogTables() {
    var query = $('#catalog-query').value.trim();
    var terms = splitTableQuery(query);
    var targets = state.catalog.sourceIds.map(function (sourceId) {
      return {
        sourceId: sourceId,
        databases: (state.catalog.databases[sourceId] || []).slice()
      };
    }).filter(function (target) {
      return target.databases.length > 0;
    });
    if (!query || !targets.length) return;

    state.catalog.searching = true;
    state.catalog.items = [];
    state.catalog.unrecognized = [];
    state.catalog.errors = [];
    renderCatalog();

    api.searchCatalogTables(targets, query).then(function (payload) {
      var items = Array.isArray(payload.items) ? payload.items.filter(function (item) {
        if (!item || !item.sourceId || !item.database || !item.table) return false;
        var tableName = String(item.table).toLowerCase();
        return !terms.length || terms.some(function (term) {
          return tableName.indexOf(term.toLowerCase()) !== -1;
        });
      }) : [];
      state.catalog.items = items;
      state.catalog.errors = Array.isArray(payload.errors) ? payload.errors : [];

      items.filter(function (item) {
        return item.exact;
      }).forEach(addSelectedCatalogTable);

      state.catalog.unrecognized = terms.filter(function (term) {
        return !items.some(function (item) {
          return item.exact && String(item.table).toLowerCase() === term.toLowerCase();
        });
      });
    }).catch(function (error) {
      state.catalog.items = [];
      state.catalog.unrecognized = [];
      state.catalog.errors = [error.message];
    }).then(function () {
      state.catalog.searching = false;
      renderCatalog();
    });
  }

  // ============ 页面渲染 ============
  function renderConfig() {
    $('#task-input').value = state.tasks.map(function (task) {
      return task.raw;
    }).join('\n');
    syncTaskCount();
  }

  function syncTaskCount() {
    $('#task-count').textContent = readTasksFromInput().length + ' 条';
  }

  function renderMapping() {
    $('#mapping-system').value = state.mapping.system || '';
    $('#mapping-script').value = state.mapping.script || '';
    $('#mapping-clob').value = state.mapping.clob || '';
  }

  function formatDateTime(value) {
    return value ? String(value).replace('T', ' ') : '-';
  }

  function formatDuration(startedAt, finishedAt, status) {
    if (!startedAt) return '-';
    if (!finishedAt && status !== 'running' && status !== 'stopping') return '-';
    var start = new Date(startedAt).getTime();
    var end = finishedAt ? new Date(finishedAt).getTime() : Date.now();
    if (!Number.isFinite(start) || !Number.isFinite(end)) return '-';

    var seconds = Math.max(0, Math.floor((end - start) / 1000));
    var hours = Math.floor(seconds / 3600);
    var minutes = Math.floor((seconds % 3600) / 60);
    var remainingSeconds = seconds % 60;
    if (hours) return hours + '时' + minutes + '分' + remainingSeconds + '秒';
    if (minutes) return minutes + '分' + remainingSeconds + '秒';
    return remainingSeconds + '秒';
  }

  function historyStatus(status) {
    var statusMap = {
      running: { label: '运行中', className: 'running' },
      stopping: { label: '停止中', className: 'running' },
      stopped: { label: '手动停止', className: 'danger' },
      interrupted: { label: '终态未知', className: 'danger' },
      succeeded: { label: '成功', className: 'ok' },
      failed: { label: '失败', className: 'danger' }
    };
    return statusMap[status] || { label: status || '未知', className: '' };
  }

  function renderHistory() {
    var body = $('#history-body');
    var rows = state.history.map(function (record) {
      var taskStatus = String(record.taskStatus || '');
      var displayStatus = taskStatus
        ? { label: taskStatus, className: finalClass(taskStatus) }
        : historyStatus(record.status);
      var tables = Array.isArray(record.tables) ? record.tables.join('、') : '';
      return '<tr>' +
        '<td>' + escapeHtml(formatDateTime(record.startedAt)) + '</td>' +
        '<td>' + escapeHtml(formatDuration(record.startedAt, record.finishedAt, record.status)) + '</td>' +
        '<td>' + escapeHtml(record.taskCount || 0) + '</td>' +
        '<td><span class="history-tables" title="' + escapeHtml(tables) + '">' +
          escapeHtml(tables || '-') + '</span></td>' +
        '<td><span class="pill ' + displayStatus.className + '">' +
          escapeHtml(displayStatus.label) + '</span></td>' +
        '<td>' + escapeHtml(record.returnCode == null ? '-' : record.returnCode) + '</td>' +
        '<td><button class="btn btn-ghost btn-sm" data-history-detail="' +
          escapeHtml(record.runId) + '">查看详情</button></td>' +
        '</tr>';
    }).join('');

    body.innerHTML = rows || (
      '<tr><td colspan="7" class="muted" style="text-align:center;padding:24px;">' +
      '暂无运行记录</td></tr>'
    );
  }

  function loadHistory(showError) {
    return api.fetchPipelineHistory().then(function (payload) {
      state.history = payload.records || [];
      renderHistory();
      return state.history;
    }).catch(function (error) {
      if (showError) {
        notify('读取运行记录失败：' + error.message, 'error');
      }
      throw error;
    });
  }

  function stageClass(status) {
    var text = String(status || '');
    if (
      text.indexOf('失败') !== -1 ||
      text.indexOf('中断') !== -1 ||
      text.indexOf('终止') !== -1 ||
      text.indexOf('崩溃') !== -1
    ) {
      return 'danger';
    }
    if (text.indexOf('进行中') !== -1 || text.indexOf('停止中') !== -1) {
      return 'running';
    }
    if (
      text.indexOf('成功') !== -1 ||
      text.indexOf('完成') !== -1 ||
      text.indexOf('已执行') !== -1 ||
      text.indexOf('已处于停止') !== -1
    ) {
      return 'ok';
    }
    if (text.indexOf('跳过') !== -1) {
      return 'warn';
    }
    return '';
  }

  function finalClass(status) {
    var text = String(status || '');
    if (text.indexOf('完美完成') !== -1 || text.indexOf('初始化完成') !== -1) {
      return 'ok';
    }
    if (text.indexOf('进行中') !== -1 || text.indexOf('正在停止') !== -1) {
      return 'running';
    }
    if (
      text.indexOf('中断') !== -1 ||
      text.indexOf('失败') !== -1 ||
      text.indexOf('终止') !== -1 ||
      text.indexOf('手动停止') !== -1
    ) {
      return 'danger';
    }
    if (text.indexOf('跳过') !== -1) {
      return 'warn';
    }
    return '';
  }

  function isFailureStatus(status) {
    var text = String(status || '');
    return text.indexOf('失败') !== -1 ||
      text.indexOf('中断') !== -1 ||
      text.indexOf('终止') !== -1 ||
      text.indexOf('崩溃') !== -1;
  }

  function isFailedResult(result) {
    return isFailureStatus(result.final) || STAGES.some(function (stage) {
      return isFailureStatus(result.stages && result.stages[stage.key]);
    });
  }

  function renderResults() {
    var body = $('#result-body');
    var rows = state.results.map(function (result) {
      var stageCells = STAGES.map(function (stage) {
        var status = result.stages && result.stages[stage.key] ? result.stages[stage.key] : '待执行';
        return '<td><span class="pill ' + stageClass(status) + '">' +
          escapeHtml(status) + '</span></td>';
      }).join('');

      var retryButton = isFailedResult(result) && !state.running
        ? '<button class="btn btn-danger btn-sm" data-retry="' +
          escapeHtml(result.id) + '">失败重跑</button>'
        : '';
      return '<tr>' +
        '<td>' + escapeHtml(result.alias) + '</td>' +
        '<td>' + escapeHtml(result.table) + '</td>' +
        '<td>' + escapeHtml(result.opLabel) + '</td>' +
        stageCells +
        '<td><span class="pill ' + finalClass(result.final) + '">' +
          escapeHtml(result.final) + '</span></td>' +
        '<td><div class="result-actions">' + retryButton +
          '<button class="btn btn-ghost btn-sm" data-detail="' +
          escapeHtml(result.id) + '">详情</button></div></td>' +
        '</tr>';
    }).join('');

    body.innerHTML = rows || (
      '<tr><td colspan="10" class="muted" style="text-align:center;padding:24px;">' +
      '暂无真实运行记录</td></tr>'
    );
    updateSummary();
  }

  function updateSummary() {
    var pending = 0;
    var running = 0;
    var succeeded = 0;
    var failed = 0;

    state.results.forEach(function (result) {
      var finalStatus = String(result.final || '');
      if (
        finalStatus.indexOf('进行中') !== -1 ||
        finalStatus.indexOf('正在停止') !== -1
      ) {
        running++;
      } else if (
        finalStatus.indexOf('完美完成') !== -1 ||
        finalStatus.indexOf('初始化完成') !== -1
      ) {
        succeeded++;
      } else if (
        finalStatus.indexOf('中断') !== -1 ||
        finalStatus.indexOf('失败') !== -1 ||
        finalStatus.indexOf('终止') !== -1 ||
        finalStatus.indexOf('跳过') !== -1 ||
        finalStatus.indexOf('手动停止') !== -1
      ) {
        failed++;
      } else {
        pending++;
      }
    });

    // 通过稳定 ID 更新摘要，避免依赖 DOM 顺序，也让读屏器能感知数值变化。
    $('#stat-pending').textContent = pending;
    $('#stat-running').textContent = running;
    $('#stat-succeeded').textContent = succeeded;
    $('#stat-failed').textContent = failed;
  }

  function renderPipelineStatus(payload) {
    state.running = payload.status === 'running' || payload.status === 'stopping';
    state.stopping = payload.status === 'stopping';
    state.runId = payload.runId || null;
    state.results = payload.results || [];
    setRunningUI(state.running, state.stopping);
    renderResults();

    $('#log-panel').textContent = payload.log || '等待启动真实流水线…';
    $('#log-panel').scrollTop = $('#log-panel').scrollHeight;

    var statusText = {
      idle: '暂无运行',
      running: '真实流水线运行中',
      stopping: '正在手动停止流水线…',
      stopped: '流水线已手动停止，退出码 ' + payload.returnCode,
      succeeded: '运行完成，退出码 0',
      failed: '运行失败，退出码 ' + payload.returnCode
    };
    $('#log-meta').textContent = statusText[payload.status] || payload.status;

    // 页面刚提交的任务始终保持清空；其他运行仍以正式 resource.text 为准。
    // 这避免快速失败时轮询将已提交任务重新填回，造成“没有清空”的错觉。
    var isCurrentSubmission = Boolean(
      payload.runId && state.submittedRunId === payload.runId
    );
    if (!state.running && payload.resourceText !== undefined && !isCurrentSubmission) {
      state.tasks = String(payload.resourceText)
        .split(/\r?\n/)
        .map(parseTaskLine)
        .filter(Boolean);
      renderConfig();
    }

    if (
      !state.running &&
      payload.runId &&
      state.completedRunId !== payload.runId &&
      (payload.status === 'succeeded' || payload.status === 'failed' || payload.status === 'stopped')
    ) {
      state.completedRunId = payload.runId;
      stopPolling();
      loadHistory(false).catch(function () {
        // 运行结果仍可在监控页查看，历史列表稍后可手动刷新。
      });
      if (payload.status === 'succeeded') {
        notify('真实流水线进程已结束。请在运行监控中查看每张表的综合终态。', 'success');
      } else if (payload.status === 'stopped') {
        notify('真实流水线已手动停止。已执行的生产操作不会自动回滚，请检查运行日志和任务状态。', 'warning', 7000);
      } else {
        notify('真实流水线执行失败，请查看运行日志。', 'error', 7000);
      }
    }
  }

  function setRunningUI(running, stopping) {
    $('#btn-run').disabled = running;
    $('#btn-run').textContent = running ? '真实流水线运行中…' : '▶ 启动真实流水线';
    $('#btn-stop').hidden = !running;
    $('#btn-stop').disabled = Boolean(stopping);
    $('#btn-stop').textContent = stopping ? '正在停止…' : '■ 手动停止入湖';
    $('#btn-validate').disabled = running;
    $('#btn-save').disabled = running;
    $('#btn-save-mapping').disabled = running;
    $('#btn-add-example').disabled = running;
    $('#task-input').disabled = running;
    $('#mapping-system').disabled = running;
    $('#mapping-script').disabled = running;
    $('#mapping-clob').disabled = running;
    updateCatalogControls();
    renderSourceManagement();
  }

  // ============ 真实状态轮询 ============
  function pollPipelineStatus(showError) {
    return api.fetchPipelineStatus().then(function (payload) {
      renderPipelineStatus(payload);
      if (payload.status === 'running' || payload.status === 'stopping') {
        startPolling();
      }
      return payload;
    }).catch(function (error) {
      if (showError) {
        notify('读取流水线状态失败：' + error.message, 'error');
      }
      throw error;
    });
  }

  function startPolling() {
    if (state.pollTimer) return;
    state.pollTimer = window.setInterval(function () {
      pollPipelineStatus(false).catch(function () {
        // 短暂请求失败时保留下一轮轮询，不把真实运行误判为停止。
      });
    }, POLL_INTERVAL_MS);
  }

  function stopPolling() {
    if (!state.pollTimer) return;
    window.clearInterval(state.pollTimer);
    state.pollTimer = null;
  }

  // ============ 视图与详情 ============
  var PAGE_TITLES = {
    config: {
      title: '任务配置',
      subtitle: '维护正式 resource.text，并启动真实入湖流水线'
    },
    monitor: {
      title: '运行监控',
      subtitle: '查看 run_pipeline.py 的实时日志与逐表结果'
    },
    history: {
      title: '运行记录',
      subtitle: '查询历史任务、运行结果与留存日志'
    },
    sources: {
      title: '数据源管理',
      subtitle: '管理安全托管的数据源连接与流水线初始化脚本'
    },
    mapping: {
      title: '映射管理',
      subtitle: '维护正式系统映射、任务脚本映射与 CLOB 白名单'
    },
    seatunnel: {
      title: 'SeaTunnel 任务',
      subtitle: '管理正式 SeaTunnel 引擎任务的启动、停止、重启与延迟监控'
    },
    rerun: {
      title: '数据重跑',
      subtitle: '直接从 ORI 重新灌数到测试或生产 StarRocks ODS 表'
    }
  };

  function switchView(view) {
    state.view = view;
    document.querySelectorAll('.nav-item').forEach(function (button) {
      var active = button.dataset.view === view;
      button.classList.toggle('is-active', active);
      if (active) {
        button.setAttribute('aria-current', 'page');
      } else {
        button.removeAttribute('aria-current');
      }
    });
    document.querySelectorAll('.view').forEach(function (section) {
      section.classList.toggle('is-active', section.id === 'view-' + view);
    });
    $('#page-title').textContent = PAGE_TITLES[view].title;
    $('#page-subtitle').textContent = PAGE_TITLES[view].subtitle;
    document.title = PAGE_TITLES[view].title + '｜入湖流水线控制台';
    if (view === 'sources' && !state.sourceManagement.loaded && !state.sourceManagement.loading) {
      loadSourceManagement().catch(function () {
        // 管理视图内已显示加载错误，保留页面供用户刷新重试。
      });
    }
    if (view === 'seatunnel') {
      if (!state.seatunnel.loading) {
        loadSeatunnel(false).then(function () {
          startSeatunnelPolling();
        }).catch(function () {
          // 列表页已显示错误提示，保留页面供用户刷新重试。
        });
      }
    } else {
      // 离开 SeaTunnel 视图时停止自动刷新，避免后台空转请求。
      stopSeatunnelPolling();
    }
    if (view === 'rerun' && !state.rerun.loaded && !state.rerun.loading) {
      loadRerun().catch(function () {
        // 视图内已显示加载错误，保留页面供用户刷新重试。
      });
    }
  }

  // 统一打开抽屉的行为，保证焦点管理和 ARIA 状态在所有详情场景中一致。
  function showDrawer() {
    state.drawerReturnFocus = document.activeElement;
    $('#drawer').setAttribute('aria-hidden', 'false');
    $('#drawer').classList.add('is-open');
    $('#drawer-backdrop').classList.add('is-open');
    window.setTimeout(function () {
      $('#drawer-close').focus();
    }, 0);
  }

  function openDrawer(resultId) {
    var result = state.results.find(function (item) {
      return String(item.id) === String(resultId);
    });
    if (!result) return;

    $('#drawer').classList.remove('is-history');
    $('#drawer-title').textContent = result.alias + ' / ' + result.table;
    var rows = [
      ['源表名', result.table],
      ['别名', result.alias],
      ['操作类型', result.opLabel]
    ].concat(STAGES.map(function (stage) {
      return [
        stage.label,
        result.stages && result.stages[stage.key] ? result.stages[stage.key] : '待执行'
      ];
    })).concat([['最终结果', result.final]]);

    $('#drawer-body').innerHTML = '<dl>' + rows.map(function (pair) {
      return '<div class="detail-row"><dt>' + escapeHtml(pair[0]) +
        '</dt><dd>' + escapeHtml(pair[1]) + '</dd></div>';
    }).join('') + '</dl>';

    showDrawer();
  }

  function retryFailedResult(resultId) {
    var result = state.results.find(function (item) {
      return String(item.id) === String(resultId);
    });
    if (!result || state.running || !isFailedResult(result)) return;

    var failedStages = STAGES.filter(function (stage) {
      return isFailureStatus(result.stages && result.stages[stage.key]);
    }).map(function (stage) {
      return stage.label;
    });
    var taskText = result.alias + ' ' + result.table + ' ' + result.opLabel + '\n';
    var confirmed = window.confirm(
      '确认重跑失败任务吗？\n\n' +
      '任务：' + result.alias + ' / ' + result.table + '\n' +
      '失败阶段：' + (failedStages.join('、') || result.final) + '\n\n' +
      '为保证阶段依赖和生产安全，本次会重跑该表的完整流水线，而不是只跳转到失败阶段。\n' +
      '执行过程中可能修改测试及生产表结构，并停止、启动 SeaTunnel 任务。'
    );
    if (!confirmed) return;

    state.running = true;
    state.completedRunId = null;
    renderResults();
    setRunningUI(true);
    switchView('monitor');
    $('#log-panel').textContent = '失败任务已提交，等待 run_pipeline.py 输出…';
    $('#log-meta').textContent = '失败任务重跑中';

    api.runPipeline(taskText).then(function (payload) {
      state.runId = payload.runId;
      state.submittedRunId = payload.runId;
      startPolling();
      return pollPipelineStatus(false);
    }).catch(function (error) {
      state.running = false;
      renderResults();
      setRunningUI(false);
      notify('失败任务重跑提交失败：' + error.message, 'error');
    });
  }

  function openHistoryDrawer(runId) {
    $('#drawer').classList.add('is-history');
    $('#drawer-title').textContent = '运行记录详情';
    $('#drawer-body').innerHTML = '<p class="muted">正在加载运行记录…</p>';
    showDrawer();

    api.fetchPipelineHistoryDetail(runId).then(function (record) {
      var displayStatus = historyStatus(record.status);
      var rows = [
        ['运行 ID', record.runId],
        ['运行状态', displayStatus.label],
        ['开始时间', formatDateTime(record.startedAt)],
        ['结束时间', formatDateTime(record.finishedAt)],
        ['运行耗时', formatDuration(record.startedAt, record.finishedAt, record.status)],
        ['退出码', record.returnCode == null ? '-' : record.returnCode]
      ];
      if (record.error) {
        rows.push(['错误信息', record.error]);
      }

      var results = Array.isArray(record.results) ? record.results : [];
      var resultRows = results.map(function (result) {
        return '<tr>' +
          '<td>' + escapeHtml(result.alias) + '</td>' +
          '<td>' + escapeHtml(result.table) + '</td>' +
          '<td>' + escapeHtml(result.opLabel) + '</td>' +
          '<td><span class="pill ' + finalClass(result.final) + '">' +
            escapeHtml(result.final) + '</span></td>' +
          '</tr>';
      }).join('');

      $('#drawer-title').textContent = '运行记录 / ' + String(record.runId || '').slice(0, 8);
      $('#drawer-body').innerHTML =
        '<dl>' + rows.map(function (pair) {
          return '<div class="detail-row"><dt>' + escapeHtml(pair[0]) +
            '</dt><dd>' + escapeHtml(pair[1]) + '</dd></div>';
        }).join('') + '</dl>' +
        '<section class="detail-section"><h3>逐表结果</h3>' +
          '<div class="table-wrap"><table class="history-result-list">' +
            '<thead><tr><th>别名</th><th>源表名</th><th>操作</th><th>最终结果</th></tr></thead>' +
            '<tbody>' + (resultRows || '<tr><td colspan="4" class="muted">暂无逐表结果</td></tr>') +
            '</tbody></table></div></section>' +
        '<section class="detail-section"><h3>运行日志</h3>' +
          '<pre class="log history-log">' + escapeHtml(record.log || '暂无运行日志') + '</pre>' +
        '</section>';
    }).catch(function (error) {
      $('#drawer-body').innerHTML = '<p class="muted">加载运行记录失败：' +
        escapeHtml(error.message) + '</p>';
    });
  }

  function closeDrawer() {
    $('#drawer').classList.remove('is-open', 'is-history');
    $('#drawer-backdrop').classList.remove('is-open');
    $('#drawer').setAttribute('aria-hidden', 'true');
    if (state.drawerReturnFocus && typeof state.drawerReturnFocus.focus === 'function') {
      state.drawerReturnFocus.focus();
    }
    state.drawerReturnFocus = null;
  }

  // ============ SeaTunnel 任务管理 ============
  function seatunnelStatusInfo(status) {
    var value = String(status || 'NOT_RUNNING').toUpperCase();
    var statusMap = {
      'RUNNING': { label: '运行中', className: 'running' },
      'INITIALIZING': { label: '初始化中', className: 'running' },
      'RECONCILING': { label: '协调中', className: 'running' },
      'DOING_SAVEPOINT': { label: '保存点制作中', className: 'warn' },
      'SUSPENDING': { label: '挂起中', className: 'warn' },
      'FINISHED': { label: '已完成', className: 'ok' },
      'SAVEPOINT_DONE': { label: '已停止', className: 'ok' },
      'FAILED': { label: '失败', className: 'danger' },
      'FAILING': { label: '失败中', className: 'danger' },
      'CANCELLING': { label: '取消中', className: 'danger' },
      'CANCELLED': { label: '已取消', className: 'danger' },
      'SUSPENDED': { label: '已挂起', className: 'danger' },
      'NOT_RUNNING': { label: '未运行', className: '' },
      'UNKNOWN': { label: '未知', className: '' }
    };
    return statusMap[value] || { label: value || '未知', className: '' };
  }

  // 判断任务是否处于“运行中”状态，用于置灰启动按钮。与后端 start_job 的
  // SEATUNNEL_ACTIVE_STATUSES 保持一致。
  function isSeatunnelActive(status) {
    var activeStatuses = [
      'RUNNING',
      'INITIALIZING',
      'RECONCILING',
      'DOING_SAVEPOINT',
      'SUSPENDING'
    ];
    return activeStatuses.indexOf(String(status || '').toUpperCase()) !== -1;
  }

  // 判断任务是否处于“正在切换/进行中”的中间态，用于展示旋转齿轮。
  function isSeatunnelTransitioning(status) {
    var value = String(status || '').toUpperCase();
    return ['INITIALIZING', 'RECONCILING', 'DOING_SAVEPOINT', 'SUSPENDING'].indexOf(value) !== -1;
  }

  // 引擎日志中的 delayTime 单位是毫秒，这里先转换为秒再格式化。
  function delayMsToSeconds(delayMs) {
    if (delayMs == null || isNaN(delayMs)) return null;
    var seconds = Math.floor(Number(delayMs) / 1000);
    return seconds < 0 ? null : seconds;
  }

  function formatDelay(delayMs) {
    var seconds = delayMsToSeconds(delayMs);
    if (seconds == null) return '-';
    if (seconds < 60) return seconds + ' 秒';
    if (seconds < 3600) return Math.floor(seconds / 60) + ' 分 ' + (seconds % 60) + ' 秒';
    if (seconds < 86400) {
      return Math.floor(seconds / 3600) + ' 小时 ' + Math.floor((seconds % 3600) / 60) + ' 分';
    }
    return Math.floor(seconds / 86400) + ' 天 ' + Math.floor((seconds % 86400) / 3600) + ' 小时';
  }

  function formatDelayShort(delayMs) {
    var seconds = delayMsToSeconds(delayMs);
    if (seconds == null) return '-';
    if (seconds < 60) return seconds + ' 秒';
    if (seconds < 3600) return Math.floor(seconds / 60) + ' 分';
    if (seconds < 86400) return (seconds / 3600).toFixed(1) + ' 时';
    return (seconds / 86400).toFixed(1) + ' 天';
  }

  function formatCount(value) {
    if (value == null || isNaN(value)) return '-';
    var num = Number(value);
    if (num >= 100000000) return (num / 100000000).toFixed(2) + ' 亿';
    if (num >= 10000) return (num / 10000).toFixed(2) + ' 万';
    return String(num);
  }

  function renderSeatunnelNodes() {
    var management = state.seatunnel;
    var body = $('#seatunnel-node-body');
    if (!body) return;
    if (management.loading && !management.nodes.length) {
      body.innerHTML = '<tr><td colspan="7" class="muted seatunnel-node-empty">正在探测集群节点…</td></tr>';
    } else if (!management.nodes.length) {
      body.innerHTML = '<tr><td colspan="7" class="muted seatunnel-node-empty">' +
        escapeHtml(management.nodeError || '暂无集群节点') + '</td></tr>';
    } else {
      body.innerHTML = management.nodes.map(function (node) {
        var online = node.status === 'ONLINE';
        var degraded = node.status === 'DEGRADED';
        var statusClass = online ? 'ok' : (degraded ? 'warn' : 'danger');
        var statusLabel = online ? '在线' : (degraded ? '异常' : '离线');
        var safeLabel = node.clusterSafe === true ? '安全' : (node.clusterSafe === false ? '不安全' : '-');
        return '<tr>' +
          '<td><span class="job-name">' + escapeHtml((node.host || '-') + ':' + (node.port || '-')) + '</span>' +
            (node.localMember ? ' <span class="pill">入口节点</span>' : '') +
            (node.error ? '<small class="seatunnel-node-error" title="' + escapeHtml(node.error) + '">探测失败</small>' : '') + '</td>' +
          '<td>' + escapeHtml(node.role || '-') + '</td>' +
          '<td><span class="pill ' + statusClass + '">' + statusLabel + '</span></td>' +
          '<td>' + escapeHtml(node.nodeState || '-') + '</td>' +
          '<td>' + escapeHtml(node.clusterState || '-') + ' / ' + escapeHtml(safeLabel) + '</td>' +
          '<td>' + escapeHtml(node.version || '-') + '</td>' +
          '<td>' + escapeHtml(node.responseMs == null ? '-' : node.responseMs + ' ms') + '</td>' +
          '</tr>';
      }).join('');
    }
    var summary = management.nodeSummary || {};
    $('#st-node-total').textContent = summary.total == null ? '-' : summary.total;
    $('#st-node-online').textContent = summary.online == null ? '-' : summary.online;
    $('#st-node-data').textContent = summary.dataMembers == null ? '-' : summary.dataMembers;
    $('#st-node-lite').textContent = summary.liteMembers == null ? '-' : summary.liteMembers;
    $('#seatunnel-node-feedback').textContent = management.nodeError
      ? '节点读取失败'
      : ((summary.online == null ? 0 : summary.online) + '/' + (summary.total == null ? 0 : summary.total) +
        ' 在线 · 连接 ' + (summary.connectionCount == null ? '-' : summary.connectionCount) +
        ' · ' + formatDateTime(management.nodesCheckedAt));
  }

  function renderSeatunnel() {
    var management = state.seatunnel;
    var body = $('#seatunnel-body');
    if (!body) return;

    if (management.loading && !management.jobs.length) {
      body.innerHTML = '<tr><td colspan="7" class="muted" style="text-align:center;padding:24px;">正在读取 SeaTunnel 任务…</td></tr>';
    } else if (!management.jobs.length) {
      body.innerHTML = '<tr><td colspan="7" class="muted" style="text-align:center;padding:24px;">暂无 SeaTunnel 任务</td></tr>';
    } else {
      body.innerHTML = management.jobs.map(function (job) {
        var status = seatunnelStatusInfo(job.status);
        var pending = management.pendingActions.indexOf(job.name) !== -1;
        var transitioning = isSeatunnelTransitioning(job.status);
        var disabled = pending ? ' disabled' : '';
        var startDisabled = pending || isSeatunnelActive(job.status) ? ' disabled' : '';
        var logDisabled = management.actionLogs[job.name] ? '' : ' disabled';

        var statusCell = ((pending || transitioning)
          ? '<span class="gear-spin" aria-hidden="true">⚙</span>'
          : '') +
          '<span class="pill ' + status.className + '" title="' + escapeHtml(job.status || '') + '">' +
          escapeHtml(status.label) + '</span>';

        var actionsCell = pending
          ? '<div class="seatunnel-actions"><span class="gear-spin" aria-hidden="true">⚙</span>' +
            '<span class="muted">操作中…</span>' +
            '<button class="btn btn-ghost btn-sm" type="button" data-st-action="log" data-st-name="' +
              escapeHtml(job.name) + '"' + logDisabled + '>查看日志</button></div>'
          : '<div class="seatunnel-actions">' +
              '<button class="btn btn-secondary btn-sm" type="button" data-st-action="start" data-st-name="' +
                escapeHtml(job.name) + '"' + startDisabled + '>启动</button>' +
              '<button class="btn btn-ghost btn-sm" type="button" data-st-action="stop" data-st-name="' +
                escapeHtml(job.name) + '"' + disabled + '>停止</button>' +
              '<button class="btn btn-ghost btn-sm" type="button" data-st-action="restart" data-st-name="' +
                escapeHtml(job.name) + '"' + disabled + '>重启</button>' +
              '<button class="btn btn-ghost btn-sm" type="button" data-st-action="config" data-st-name="' +
                escapeHtml(job.name) + '">查看配置</button>' +
              '<button class="btn btn-ghost btn-sm" type="button" data-st-action="log" data-st-name="' +
                escapeHtml(job.name) + '"' + logDisabled + '>查看日志</button>' +
            '</div>';

        return '<tr>' +
          '<td><span class="job-name" title="' + escapeHtml(job.configFile || job.name) + '">' +
            escapeHtml(job.name) + '</span></td>' +
          '<td>' + escapeHtml(job.mode || '-') + '</td>' +
          '<td>' + statusCell + '</td>' +
          '<td><span class="delay-cell">' + escapeHtml(formatDelay(job.delayMs)) + '</span></td>' +
          '<td>' + escapeHtml(formatCount(job.sourceReceivedCount)) + '</td>' +
          '<td>' + escapeHtml(formatCount(job.sinkWriteCount)) + '</td>' +
          '<td>' + actionsCell + '</td>' +
        '</tr>';
      }).join('');
    }

    var total = management.jobs.length;
    var runningCount = management.jobs.filter(function (job) {
      return seatunnelStatusInfo(job.status).className === 'running';
    }).length;
    var failedCount = management.jobs.filter(function (job) {
      return seatunnelStatusInfo(job.status).className === 'danger';
    }).length;
    var delays = management.jobs.map(function (job) {
      return Number(job.delayMs);
    }).filter(function (value) {
      return !isNaN(value) && value >= 0;
    });
    var maxDelay = delays.length ? Math.max.apply(null, delays) : null;

    $('#st-stat-total').textContent = total;
    $('#st-stat-running').textContent = runningCount;
    $('#st-stat-failed').textContent = failedCount;
    $('#st-stat-delay').textContent = maxDelay == null ? '-' : formatDelayShort(maxDelay);
    $('#seatunnel-feedback').textContent = management.loading
      ? '刷新中…'
      : (management.polling ? '自动刷新中…' : (total ? total + ' 个任务' : '无任务'));
    $('#btn-seatunnel-refresh').textContent = management.loading ? '刷新中…' : '刷新';
    $('#btn-seatunnel-refresh').disabled = Boolean(management.loading);
    renderSeatunnelNodes();
  }

  function loadSeatunnel(showError) {
    var management = state.seatunnel;
    management.loading = true;
    renderSeatunnel();
    var jobsRequest = api.fetchSeatunnelJobs();
    var nodesRequest = api.fetchSeatunnelNodes().then(function (payload) {
      management.nodes = Array.isArray(payload.nodes) ? payload.nodes : [];
      management.nodeSummary = payload.summary || {};
      management.nodesCheckedAt = payload.checkedAt || null;
      management.nodeError = null;
    }).catch(function (error) {
      management.nodeError = error.message;
      if (showError) notify('读取 SeaTunnel 节点失败：' + error.message, 'error');
    });
    return Promise.all([jobsRequest, nodesRequest]).then(function (responses) {
      var payload = responses[0];
      management.jobs = Array.isArray(payload.jobs) ? payload.jobs : [];
      management.loaded = true;
      management.loading = false;
      renderSeatunnel();
      return management.jobs;
    }).catch(function (error) {
      management.loading = false;
      renderSeatunnel();
      if (showError) {
        notify('读取 SeaTunnel 任务失败：' + error.message, 'error');
      }
      throw error;
    });
  }

  function startSeatunnelPolling() {
    var management = state.seatunnel;
    if (management.pollTimer) return;

    management.polling = true;
    management.pollDeadline = Date.now() + SEATUNNEL_POLL_MAX_MS;

    var refresh = function () {
      if (Date.now() >= management.pollDeadline) {
        stopSeatunnelPolling();
        return;
      }
      Promise.all([api.fetchSeatunnelJobs(), api.fetchSeatunnelNodes()]).then(function (responses) {
        var jobsPayload = responses[0];
        var nodesPayload = responses[1];
        management.jobs = Array.isArray(jobsPayload.jobs) ? jobsPayload.jobs : [];
        management.nodes = Array.isArray(nodesPayload.nodes) ? nodesPayload.nodes : [];
        management.nodeSummary = nodesPayload.summary || {};
        management.nodesCheckedAt = nodesPayload.checkedAt || null;
        management.nodeError = null;
        management.loaded = true;
        renderSeatunnel();
      }).catch(function () {
        // 单次刷新失败不中断后续轮询，保留现有数据供用户手动刷新。
      });
    };

    // 立即刷新一次，再进入定时轮询。
    refresh();
    management.pollTimer = window.setInterval(refresh, SEATUNNEL_POLL_INTERVAL_MS);
    renderSeatunnel();
  }

  function stopSeatunnelPolling() {
    var management = state.seatunnel;
    if (!management.pollTimer) return;
    window.clearInterval(management.pollTimer);
    management.pollTimer = null;
    management.polling = false;
    renderSeatunnel();
  }

  function openSeatunnelConfig(name) {
    $('#drawer').classList.remove('is-history');
    $('#drawer-title').textContent = 'SeaTunnel 配置 / ' + name;
    $('#drawer-body').innerHTML = '<p class="muted">正在加载配置文件…</p>';
    showDrawer();

    api.fetchSeatunnelConfig(name).then(function (payload) {
      $('#drawer-title').textContent = 'SeaTunnel 配置 / ' + payload.configFile;
      $('#drawer-body').innerHTML =
        '<p class="muted config-note">以下为当前任务的运行配置，只读、不可编辑。</p>' +
        '<pre class="config-viewer">' + escapeHtml(payload.content || '') + '</pre>';
    }).catch(function (error) {
      $('#drawer-body').innerHTML = '<p class="muted">加载配置失败：' +
        escapeHtml(error.message) + '</p>';
    });
  }

  function showSeatunnelLogModal() {
    $('#log-modal').classList.remove('is-fullscreen');
    $('#log-modal-fullscreen').textContent = '全屏';
    $('#log-modal').classList.add('is-open');
    $('#log-modal-backdrop').classList.add('is-open');
    $('#log-modal').setAttribute('aria-hidden', 'false');
    window.setTimeout(function () {
      $('#log-modal-close').focus();
    }, 0);
  }

  function closeSeatunnelLogModal() {
    $('#log-modal').classList.remove('is-open', 'is-fullscreen');
    $('#log-modal-backdrop').classList.remove('is-open');
    $('#log-modal').setAttribute('aria-hidden', 'true');
    $('#log-modal-fullscreen').textContent = '全屏';
    state.seatunnel.viewingOperationId = null;
    state.rerun.viewingOperationId = null;
  }

  function toggleSeatunnelLogFullscreen() {
    var fullscreen = $('#log-modal').classList.toggle('is-fullscreen');
    $('#log-modal-fullscreen').textContent = fullscreen ? '退出全屏' : '全屏';
  }

  function openSeatunnelActionLog(name, label, output) {
    $('#log-modal-title').textContent = '操作日志 / ' + label + ' ' + name;
    $('#log-modal-status').textContent = '已完成';
    $('#log-modal-content').textContent = output || '（无日志输出）';
    showSeatunnelLogModal();
  }

  function stopSeatunnelOperationPolling() {
    var management = state.seatunnel;
    if (management.operationPollTimer) {
      window.clearInterval(management.operationPollTimer);
      management.operationPollTimer = null;
    }
  }

  function startSeatunnelOperationPolling(name, operationId) {
    var management = state.seatunnel;
    stopSeatunnelOperationPolling();

    var timer = window.setInterval(function () {
      api.fetchSeatunnelOperationLog(operationId).then(function (op) {
        // 只有用户正在查看该操作时才更新可见弹窗，避免自动弹出。
        if (state.seatunnel.viewingOperationId === operationId) {
          var content = $('#log-modal-content');
          content.textContent = op.log || '（暂无日志输出）';
          content.scrollTop = content.scrollHeight;
          $('#log-modal-status').textContent = op.status === 'running'
            ? '实时执行中'
            : (op.status === 'succeeded' ? '已完成' : '失败');
        }

        if (op.status !== 'running') {
          window.clearInterval(timer);
          if (management.operationPollTimer === timer) {
            management.operationPollTimer = null;
          }

          if (management.actionLogs[name]) {
            management.actionLogs[name].output = op.log;
            management.actionLogs[name].error = op.error;
          }
          management.pendingActions = management.pendingActions.filter(function (item) {
            return item !== name;
          });
          renderSeatunnel();

          if (op.status === 'succeeded') {
            notify('任务操作完成。', 'success');
          } else {
            notify('任务操作失败：' + (op.error || '未知错误'), 'error', 7000);
          }
          // 操作结束后刷新任务列表，跟踪远程引擎状态变化。
          startSeatunnelPolling();
        }
      }).catch(function () {
        // 单次读取失败不中断轮询。
      });
    }, 1000);

    management.operationPollTimer = timer;
  }

  function openSeatunnelStoredLog(name) {
    var entry = state.seatunnel.actionLogs[name];
    if (!entry) return;
    if (!entry.operationId) {
      openSeatunnelActionLog(name, entry.label, entry.output);
      return;
    }
    api.fetchSeatunnelOperationLog(entry.operationId).then(function (op) {
      if (op.status === 'running') {
        // 标记正在查看的操作并打开实时视图；后台轮询会持续更新弹窗。
        state.seatunnel.viewingOperationId = entry.operationId;
        $('#log-modal-title').textContent = '操作日志 / ' + name;
        $('#log-modal-status').textContent = '实时执行中';
        $('#log-modal-content').textContent = op.log || '正在执行，等待日志输出…';
        showSeatunnelLogModal();
        // 若后台轮询未运行（例如页面刷新后），则启动。
        if (!state.seatunnel.operationPollTimer) {
          startSeatunnelOperationPolling(name, entry.operationId);
        }
      } else {
        openSeatunnelActionLog(name, entry.label, op.log || entry.output);
      }
    }).catch(function () {
      openSeatunnelActionLog(name, entry.label, entry.output);
    });
  }

  function seatunnelAction(name, action) {
    var management = state.seatunnel;
    if (management.pendingActions.indexOf(name) !== -1) return;

    var actionLabels = { start: '启动', stop: '停止', restart: '重启' };
    var label = actionLabels[action] || action;
    var confirmed = window.confirm('确认' + label + ' SeaTunnel 任务「' + name + '」吗？\n\n此操作会直接作用于生产 SeaTunnel 引擎。');
    if (!confirmed) return;

    management.pendingActions.push(name);
    renderSeatunnel();

    var request;
    if (action === 'start') request = api.startSeatunnelJob(name);
    else if (action === 'stop') request = api.stopSeatunnelJob(name);
    else request = api.restartSeatunnelJob(name);

    request.then(function (payload) {
      var operationId = payload && payload.operationId;
      management.actionLogs[name] = {
        operationId: operationId,
        label: label,
        output: '',
        error: null
      };
      notify((payload && payload.message) || (label + '任务已提交'), 'success');
      // 重新渲染，让操作中的「查看日志」按钮变为可用。
      renderSeatunnel();
      // 开始实时拉取日志。
      startSeatunnelOperationPolling(name, operationId);
    }).catch(function (error) {
      notify(label + '任务提交失败：' + error.message, 'error', 7000);
      management.pendingActions = management.pendingActions.filter(function (item) {
        return item !== name;
      });
      renderSeatunnel();
    });
  }

  // ============ 数据重跑 ============
  function rerunTableKey(item) {
    return [item.environment, item.table].map(function (value) {
      return String(value || '').toLowerCase();
    }).join('\u0001');
  }

  function getRerunEnvironment(environmentId) {
    return state.rerun.environments.find(function (environment) {
      return environment.id === environmentId;
    }) || null;
  }

  function updateRerunControls() {
    var rerun = state.rerun;
    var unavailable = rerun.running || !rerun.available || rerun.loading || !rerun.selectedEnvironments.length;
    var selectableItems = rerun.items.filter(function (item) { return item.oriExists; });
    $('#rerun-query').disabled = unavailable || rerun.searching;
    $('#btn-rerun-search').disabled = unavailable || rerun.searching || !$('#rerun-query').value.trim();
    $('#rerun-select-all').disabled = unavailable || !selectableItems.length;
    $('#btn-rerun-run').disabled = unavailable || !Object.keys(rerun.selected).length;

    $('#btn-rerun-log').hidden = !rerun.operationId;
  }

  function renderRerun() {
    var rerun = state.rerun;
    var statusText = rerun.loading ? '环境读取中' : (rerun.available ? 'StarRocks 已连接' : rerun.status);
    if (rerun.running) statusText = '执行中 ' + rerun.completed + '/' + rerun.total;
    else if (rerun.operationStatus === 'succeeded') statusText = '最近执行成功';
    else if (rerun.operationStatus === 'failed') statusText = '最近执行有失败';
    $('#rerun-status').textContent = statusText;

    if (rerun.loading) {
      $('#rerun-environments').innerHTML = '<p class="muted">正在读取环境…</p>';
    } else if (!rerun.available) {
      $('#rerun-environments').innerHTML = '<p class="muted">' + escapeHtml(rerun.status) + '</p>';
    } else {
      $('#rerun-environments').innerHTML = rerun.environments.map(function (environment) {
        var warning = environment.production ? '<small>直接操作生产 StarRocks</small>' : '<small>建议先在测试环境验证</small>';
        return '<label class="selector-option' + (environment.production ? ' is-production' : '') + '">' +
          '<input type="checkbox" name="rerun-environment" value="' + escapeHtml(environment.id) + '"' +
            (rerun.selectedEnvironments.indexOf(environment.id) !== -1 ? ' checked' : '') + (rerun.running ? ' disabled' : '') + ' />' +
          '<span class="selector-option-text">' + escapeHtml(environment.label) + warning + '</span>' +
          '</label>';
      }).join('');
    }

    var feedback = [];

    if (rerun.errors.length) {
      feedback.push('<div class="feedback-error">' + rerun.errors.map(escapeHtml).join('；') + '</div>');
    }
    $('#rerun-feedback').innerHTML = feedback.join('');

    if (rerun.searching) {
      $('#rerun-result-body').innerHTML = '<tr><td colspan="5" class="muted rerun-empty">正在搜索 StarRocks 表…</td></tr>';
    } else if (!rerun.items.length) {
      $('#rerun-result-body').innerHTML = '<tr><td colspan="5" class="muted rerun-empty">输入 ODS 表名后搜索</td></tr>';
    } else {
      var selectableItems = rerun.items.filter(function (item) { return item.oriExists; });
      var allChecked = selectableItems.length && selectableItems.every(function (item) {
        return Boolean(rerun.selected[rerunTableKey(item)]);
      });
      $('#rerun-select-all').checked = Boolean(allChecked);
      $('#rerun-result-body').innerHTML = rerun.items.map(function (item, index) {
        var key = rerunTableKey(item);
        var checked = Boolean(rerun.selected[key]);
        var sourceState = item.oriExists
          ? '<span class="status-dot status-success">可灌数</span>'
          : '<span class="status-dot status-danger">缺少同名表</span>';
        return '<tr>' +
          '<td><input type="checkbox" data-rerun-result-index="' + index + '"' +
            (checked ? ' checked' : '') + (!item.oriExists || rerun.running ? ' disabled' : '') + ' /></td>' +
          '<td>' + escapeHtml(item.environmentLabel) + '</td>' +
          '<td><span class="job-name">' + escapeHtml(item.table) + '</span>' +
            (item.exact ? '' : ' <small class="muted">模糊</small>') + '</td>' +
          '<td><span class="status-dot status-success">存在</span></td>' +
          '<td>' + sourceState + '</td>' +
          '</tr>';
      }).join('');
    }

    var keys = Object.keys(rerun.selected);
    $('#rerun-selected-count').textContent = keys.length + ' 张';
    $('#rerun-selected-list').innerHTML = keys.map(function (key) {
      var item = rerun.selected[key];
      return '<div class="rerun-selected-item">' +
        '<span class="rerun-item-name">' + escapeHtml(item.environmentLabel) + ' / ods.' + escapeHtml(item.table) + '</span>' +
        '<button class="btn btn-ghost btn-sm" type="button" data-rerun-remove="' + escapeHtml(key) + '"' +
          (rerun.running ? ' disabled' : '') + '>移除</button>' +
        '</div>';
    }).join('') || '<span class="muted">尚未选择表</span>';

    updateRerunControls();
  }

  function renderRerunHistory() {
    var rerun = state.rerun;
    var query = rerun.historyQuery.trim().toLowerCase();
    var records = rerun.history.filter(function (record) {
      if (!query) return true;
      var tables = (record.tables || []).map(function (item) { return item.table; }).join(' ');
      var statusLabel = historyStatus(record.status).label;
      return [record.environmentLabel, record.status, statusLabel, tables].join(' ').toLowerCase().indexOf(query) !== -1;
    });
    if (rerun.historyLoading) {
      $('#rerun-history-body').innerHTML = '<tr><td colspan="7" class="muted rerun-empty">正在读取重跑记录…</td></tr>';
      return;
    }
    $('#rerun-history-body').innerHTML = records.map(function (record) {
      var status = historyStatus(record.status);
      var tableNames = (record.tables || []).map(function (item) {
        return (item.environment === 'prod' ? '生产' : '测试') + '/' + item.table;
      }).join('、');
      return '<tr>' +
        '<td>' + escapeHtml(formatDateTime(record.startedAt)) + '</td>' +
        '<td>' + escapeHtml(record.environmentLabel || '-') + '</td>' +
        '<td>' + escapeHtml(record.total || 0) + '</td>' +
        '<td>' + escapeHtml(record.failedCount || 0) + '</td>' +
        '<td><span class="history-tables" title="' + escapeHtml(tableNames) + '">' + escapeHtml(tableNames || '-') + '</span></td>' +
        '<td><span class="pill ' + status.className + '">' + escapeHtml(status.label) + '</span></td>' +
        '<td><button class="btn btn-ghost btn-sm" type="button" data-rerun-history="' +
          escapeHtml(record.operationId) + '">查看</button></td>' +
        '</tr>';
    }).join('') || '<tr><td colspan="7" class="muted rerun-empty">暂无匹配的重跑记录</td></tr>';
  }

  function loadRerunHistory(showFeedback) {
    state.rerun.historyLoading = true;
    renderRerunHistory();
    return api.fetchRerunHistory().then(function (payload) {
      state.rerun.history = Array.isArray(payload.records) ? payload.records : [];
      state.rerun.historyLoading = false;
      renderRerunHistory();
      if (showFeedback) notify('重跑记录已刷新。', 'success');
    }).catch(function (error) {
      state.rerun.historyLoading = false;
      renderRerunHistory();
      notify('读取重跑记录失败：' + error.message, 'error');
    });
  }

  function openRerunHistory(operationId) {
    api.fetchRerunHistoryDetail(operationId).then(function (operation) {
      $('#log-modal-title').textContent = '重跑记录 / ' + (operation.environmentLabel || '-');
      $('#log-modal-status').textContent = historyStatus(operation.status).label;
      $('#log-modal-content').textContent = operation.log || '（无日志输出）';
      showSeatunnelLogModal();
    }).catch(function (error) {
      notify('读取重跑记录详情失败：' + error.message, 'error');
    });
  }

  function loadRerun() {
    var rerun = state.rerun;
    rerun.loading = true;
    rerun.status = '环境读取中';
    renderRerun();
    return api.fetchRerunEnvironments().then(function (payload) {
      rerun.environments = Array.isArray(payload.environments) ? payload.environments : [];
      rerun.available = rerun.environments.length > 0;
      rerun.status = rerun.available ? 'StarRocks 已连接' : '未配置 StarRocks 环境';
      rerun.selectedEnvironments = rerun.selectedEnvironments.filter(function (environmentId) {
        return rerun.environments.some(function (item) { return item.id === environmentId; });
      });
      if (!rerun.selectedEnvironments.length && rerun.environments.length) {
        rerun.selectedEnvironments = [rerun.environments[0].id];
      }
      rerun.loaded = true;
      rerun.loading = false;
      renderRerun();
      loadRerunHistory(false);
      return rerun.environments;
    }).catch(function (error) {
      rerun.loading = false;
      rerun.available = false;
      rerun.status = '环境不可用：' + error.message;
      renderRerun();
      throw error;
    });
  }

  function searchRerunTables() {
    var rerun = state.rerun;
    var query = $('#rerun-query').value.trim();
    if (!query || rerun.running) return;
    rerun.searching = true;
    rerun.errors = [];
    renderRerun();

    Promise.all(rerun.selectedEnvironments.map(function (environmentId) {
      return api.searchRerunTables(environmentId, query).then(function (payload) {
        var environment = getRerunEnvironment(environmentId);
        return (Array.isArray(payload.items) ? payload.items : []).filter(function (item) {
          return item && item.table && item.odsExists;
        }).map(function (item) {
          return Object.assign({}, item, {
            environment: environmentId,
            environmentLabel: environment ? environment.label : environmentId
          });
        });
      });
    })).then(function (groups) {
      rerun.items = [].concat.apply([], groups);
      rerun.searching = false;
      renderRerun();
    }).catch(function (error) {
      rerun.searching = false;
      rerun.errors = ['搜索表失败：' + error.message];
      renderRerun();
    });
  }

  function stopRerunPolling() {
    if (state.rerun.operationPollTimer) {
      window.clearInterval(state.rerun.operationPollTimer);
      state.rerun.operationPollTimer = null;
    }
  }

  function applyRerunOperation(operation) {
    var rerun = state.rerun;
    rerun.running = operation.status === 'running';
    rerun.operationStatus = operation.status;
    rerun.completed = Number(operation.completed || 0);
    rerun.total = Number(operation.total || 0);
    if (rerun.viewingOperationId === operation.operationId) {
      $('#log-modal-status').textContent = operation.status === 'running'
        ? '实时执行中 ' + rerun.completed + '/' + rerun.total
        : (operation.status === 'succeeded' ? '已完成' : '失败');
      $('#log-modal-content').textContent = operation.log || '（暂无日志输出）';
      $('#log-modal-content').scrollTop = $('#log-modal-content').scrollHeight;
    }
    renderRerun();
  }

  function startRerunPolling(operationId) {
    stopRerunPolling();
    var finishedNotified = false;
    function poll() {
      api.fetchRerunStatus(operationId).then(function (operation) {
        applyRerunOperation(operation);
        if (operation.status !== 'running') {
          stopRerunPolling();
          if (!finishedNotified) {
            finishedNotified = true;
            notify(
              operation.status === 'succeeded' ? '数据重跑全部完成。' : '数据重跑完成，但有表执行失败。',
              operation.status === 'succeeded' ? 'success' : 'error',
              7000
            );
            loadRerunHistory(false);
          }
        }
      }).catch(function () {
        // 单次读取失败不中断实时日志轮询。
      });
    }
    poll();
    state.rerun.operationPollTimer = window.setInterval(poll, 1000);
  }

  function openRerunLog() {
    var operationId = state.rerun.operationId;
    if (!operationId) return;
    api.fetchRerunStatus(operationId).then(function (operation) {
      state.rerun.viewingOperationId = operationId;
      $('#log-modal-title').textContent = '数据重跑日志 / ' + operation.environmentLabel;
      $('#log-modal-status').textContent = operation.status === 'running'
        ? '实时执行中 ' + operation.completed + '/' + operation.total
        : (operation.status === 'succeeded' ? '已完成' : '失败');
      $('#log-modal-content').textContent = operation.log || '正在执行，等待日志输出…';
      showSeatunnelLogModal();
      if (operation.status === 'running' && !state.rerun.operationPollTimer) {
        startRerunPolling(operationId);
      }
    }).catch(function (error) {
      notify('读取数据重跑日志失败：' + error.message, 'error');
    });
  }

  function runRerun() {
    var rerun = state.rerun;
    var keys = Object.keys(rerun.selected);
    if (!keys.length || rerun.running) return;
    var operationEnvironmentIds = [];
    keys.forEach(function (key) {
      var environmentId = rerun.selected[key].environment;
      if (operationEnvironmentIds.indexOf(environmentId) === -1) {
        operationEnvironmentIds.push(environmentId);
      }
    });
    var environments = operationEnvironmentIds.map(getRerunEnvironment).filter(Boolean);
    if (!environments.length) return;
    var confirmed = window.confirm(
      '确认重跑' + environments.map(function (item) { return item.label; }).join('、') + '的 StarRocks 数据吗？\n\n' +
      '共 ' + keys.length + ' 张 ODS 表，将执行 INSERT OVERWRITE 从同名 ORI 表重新灌数。\n' +
      '本操作不会创建、删除或修改表结构。'
    );
    if (!confirmed) return;

    // 生产环境不再单独弹出文字确认；执行前的汇总确认仍然保留。
    var productionConfirmed = environments.some(function (item) { return item.production; });

    var tables = keys.map(function (key) {
      return {
        environment: rerun.selected[key].environment,
        table: rerun.selected[key].table
      };
    });
    rerun.running = true;
    rerun.completed = 0;
    rerun.total = tables.length;
    rerun.errors = [];
    renderRerun();

    api.runRerun(operationEnvironmentIds, tables, productionConfirmed).then(function (payload) {
      rerun.operationId = payload.operationId;
      rerun.operationStatus = 'running';
      renderRerun();
      notify('数据重跑已提交，可点击“查看实时日志”。', 'success');
      loadRerunHistory(false);
      startRerunPolling(payload.operationId);
    }).catch(function (error) {
      rerun.running = false;
      rerun.errors = ['提交失败：' + error.message];
      renderRerun();
      notify('数据重跑提交失败：' + error.message, 'error');
    });
  }

  // ============ 事件绑定 ============
  function bindEvents() {
    // 支持使用 Esc 关闭详情抽屉或日志弹窗，避免键盘用户必须定位到关闭按钮。
    document.addEventListener('keydown', function (event) {
      if (event.key !== 'Escape') return;
      if ($('#log-modal').classList.contains('is-open')) {
        closeSeatunnelLogModal();
      } else if ($('#drawer').classList.contains('is-open')) {
        closeDrawer();
      }
    });

    document.querySelectorAll('.nav-item').forEach(function (button) {
      button.addEventListener('click', function () {
        switchView(button.dataset.view);
        if (button.dataset.view === 'monitor') {
          pollPipelineStatus(false).catch(function () {
            // 页面顶部健康状态会提示后端不可用，无需重复弹窗。
          });
        } else if (button.dataset.view === 'history') {
          loadHistory(false).catch(function () {
            // 保留现有历史列表，用户可点击刷新后重试。
          });
        }
      });
    });

    $('#catalog-selector').addEventListener('change', function (event) {
      var target = event.target;
      if (target.name === 'catalog-system') {
        state.catalog.systemId = target.value;
        state.catalog.sourceIds = [];
        state.catalog.databases = {};
        state.catalog.items = [];
        state.catalog.unrecognized = [];
        state.catalog.errors = [];
        renderCatalog();
        return;
      }

      if (target.hasAttribute('data-catalog-target')) {
        var sourceId = target.dataset.catalogTarget;
        var databases = state.catalog.databases[sourceId] || [];
        if (target.checked) {
          if (state.catalog.sourceIds.indexOf(sourceId) === -1) {
            state.catalog.sourceIds.push(sourceId);
          }
          if (databases.indexOf(target.value) === -1) {
            databases.push(target.value);
          }
          state.catalog.databases[sourceId] = databases;
        } else {
          databases = databases.filter(function (database) {
            return database !== target.value;
          });
          if (databases.length) {
            state.catalog.databases[sourceId] = databases;
          } else {
            delete state.catalog.databases[sourceId];
            state.catalog.sourceIds = state.catalog.sourceIds.filter(function (id) {
              return id !== sourceId;
            });
          }
        }
        state.catalog.items = [];
        state.catalog.unrecognized = [];
        state.catalog.errors = [];
        renderCatalog();
        return;
      }

      if (target.hasAttribute('data-catalog-result-index')) {
        var item = state.catalog.items[Number(target.dataset.catalogResultIndex)];
        if (!item) return;
        if (target.checked) {
          addSelectedCatalogTable(item);
        } else {
          var itemKey = catalogTableKey(item);
          state.catalog.selectedTables = state.catalog.selectedTables.filter(function (selected) {
            return catalogTableKey(selected) !== itemKey;
          });
        }
        renderCatalog();
      }
    });

    $('#catalog-selector').addEventListener('click', function (event) {
      var button = event.target.closest('[data-selected-table-index]');
      if (!button || button.disabled) return;
      state.catalog.selectedTables.splice(Number(button.dataset.selectedTableIndex), 1);
      renderCatalog();
    });

    $('#btn-clear-selected-tables').addEventListener('click', function () {
      state.catalog.selectedTables = [];
      renderCatalog();
      $('#catalog-feedback').innerHTML = '<div>已清空所选表。</div>';
    });

    $('#catalog-query').addEventListener('input', updateCatalogControls);
    $('#catalog-search-form').addEventListener('submit', function (event) {
      event.preventDefault();
      searchCatalogTables();
    });

    $('#btn-add-selected-tasks').addEventListener('click', function () {
      var sourceWithoutPipeline = state.catalog.selectedTables.find(function (item) {
        var source = getCatalogSource(item.sourceId);
        return source && source.pipelineReady === false;
      });
      if (sourceWithoutPipeline) {
        $('#catalog-feedback').innerHTML = '<div class="feedback-warn">连接已验证，但正式初始化脚本未就绪</div>';
        return;
      }

      var operation = document.querySelector('[name="catalog-operation"]:checked').value;
      var input = $('#task-input');
      var currentText = input.value.replace(/\s+$/, '');
      var existingLines = currentText.split(/\r?\n/).filter(function (line) {
        return line.trim();
      });
      var seen = {};
      existingLines.forEach(function (line) {
        seen[line.trim().replace(/\s+/g, ' ').toLowerCase()] = true;
      });

      var addedLines = [];
      state.catalog.selectedTables.forEach(function (item) {
        var source = getCatalogSource(item.sourceId);
        var needsDatabasePrefix = source && (source.databases || []).length > 1;
        var tableName = needsDatabasePrefix ? item.database + '.' + item.table : item.table;
        var line = item.sourceId + ' ' + tableName + ' ' + operation;
        var key = line.toLowerCase();
        if (!seen[key]) {
          seen[key] = true;
          addedLines.push(line);
        }
      });

      if (addedLines.length) {
        input.value = currentText ? currentText + '\n' + addedLines.join('\n') : addedLines.join('\n');
      }
      syncTaskCount();
      state.catalog.selectedTables = [];
      renderCatalog();
      $('#catalog-feedback').innerHTML = '<div>' +
        (addedLines.length ? '已加入 ' + addedLines.length + ' 条任务，已清空所选表。' : '所选表已存在于待入湖任务中，已清空选择。') +
        '</div>';
    });

    $('#source-system-list').addEventListener('click', function (event) {
      var deleteButton = event.target.closest('[data-delete-source-system]');
      if (deleteButton && !deleteButton.disabled) {
        var deleteSystemId = deleteButton.dataset.deleteSourceSystem;
        var system = getManagedSystem(deleteSystemId);
        if (!system || !system.managed || system.readOnly || getSystemSources(deleteSystemId).length) return;
        if (!window.confirm('确认删除空业务系统「' + (system.label || deleteSystemId) + '」吗？')) return;
        performSourceMutation(function () {
          return api.deleteSourceSystem(deleteSystemId);
        }, {
          scope: 'system',
          selectedSystemId: '',
          successMessage: '业务系统已删除。'
        }).catch(function () {
          // 错误已显示在业务系统反馈区域。
        });
        return;
      }

      var item = event.target.closest('[data-source-system-id]');
      if (!item) return;
      state.sourceManagement.selectedSystemId = item.dataset.sourceSystemId;
      state.sourceManagement.feedback = null;
      state.sourceManagement.error = null;
      renderSourceManagement();
    });

    $('#source-system-list').addEventListener('keydown', function (event) {
      if (event.key !== 'Enter' && event.key !== ' ') return;
      if (event.target.closest('[data-delete-source-system]')) return;
      var item = event.target.closest('[data-source-system-id]');
      if (!item) return;
      event.preventDefault();
      state.sourceManagement.selectedSystemId = item.dataset.sourceSystemId;
      state.sourceManagement.feedback = null;
      state.sourceManagement.error = null;
      renderSourceManagement();
    });

    $('#btn-add-source-system').addEventListener('click', function () {
      var id = window.prompt('请输入业务系统 ID（2-40 位小写字母、数字或下划线，以小写字母开头）：');
      if (id == null) return;
      id = id.trim();
      if (!/^[a-z][a-z0-9_]{1,39}$/.test(id)) {
        notify('业务系统 ID 格式错误，应以小写字母开头，且为 2-40 位小写字母、数字或下划线。', 'warning');
        return;
      }
      var label = window.prompt('请输入业务系统显示名称：');
      if (label == null) return;
      label = label.trim();
      if (!label) {
        notify('业务系统显示名称不能为空。', 'warning');
        return;
      }

      performSourceMutation(function () {
        return api.createSourceSystem({ id: id, label: label });
      }, {
        scope: 'system',
        selectedSystemId: id,
        successMessage: '业务系统已创建。'
      }).catch(function () {
        // 错误已显示在业务系统反馈区域。
      });
    });

    $('#btn-refresh-sources').addEventListener('click', function () {
      state.sourceManagement.feedback = null;
      state.sourceManagement.error = null;
      loadSourceManagement().catch(function () {
        // 错误已显示在数据源反馈区域。
      });
    });

    $('#btn-add-source').addEventListener('click', function () {
      if (!getManagedSystem(state.sourceManagement.selectedSystemId) || state.running) return;
      openSourceDrawer(null);
    });

    $('#source-body').addEventListener('click', function (event) {
      var validateButton = event.target.closest('[data-validate-source]');
      if (validateButton && !validateButton.disabled) {
        validateManagedSource(validateButton.dataset.validateSource);
        return;
      }

      var editButton = event.target.closest('[data-edit-source]');
      if (editButton && !editButton.disabled) {
        var source = getManagedSource(editButton.dataset.editSource);
        if (source && source.managed && !source.readOnly) openSourceDrawer(source);
        return;
      }

      var deleteButton = event.target.closest('[data-delete-source]');
      if (!deleteButton || deleteButton.disabled) return;
      var sourceId = deleteButton.dataset.deleteSource;
      var sourceToDelete = getManagedSource(sourceId);
      if (!sourceToDelete || !sourceToDelete.managed || sourceToDelete.readOnly) return;
      if (!window.confirm(
        '确认删除数据源「' + (sourceToDelete.label || sourceId) + '」吗？\n\n' +
        '该操作只删除托管连接配置，不会清理已有任务、目标表或 SeaTunnel 配置。若 resource.text 仍引用该数据源，后端会拒绝删除。'
      )) return;

      performSourceMutation(function () {
        return api.deleteSource(sourceId);
      }, {
        scope: 'source',
        selectedSystemId: state.sourceManagement.selectedSystemId,
        successMessage: '数据源已删除。'
      }).catch(function () {
        // 错误已显示在数据源反馈区域。
      });
    });

    $('#task-input').addEventListener('input', syncTaskCount);

    $('#btn-validate').addEventListener('click', function () {
      var tasks = getValidTasksOrAlert();
      if (tasks) {
        notify('格式校验通过，共 ' + tasks.length + ' 条任务。', 'success');
      }
    });

    $('#btn-add-example').addEventListener('click', function () {
      var example = 'sub2sr ods_sub_sub_write_sr_person_attr_t 表\noa2sr FORM_INFO_T 字段';
      $('#task-input').value = $('#task-input').value ?
        $('#task-input').value + '\n' + example :
        example;
      syncTaskCount();
    });

    $('#btn-save').addEventListener('click', function () {
      var text = $('#task-input').value;
      var parsed = text.trim() ? getValidTasksOrAlert() : [];
      if (text.trim() && !parsed) return;

      api.saveTasks(text).then(function (payload) {
        state.tasks = String(payload.text || '')
          .split(/\r?\n/)
          .map(parseTaskLine)
          .filter(Boolean);
        renderConfig();
        notify('已写入正式 resource.text。', 'success');
      }).catch(function (error) {
        notify('保存任务失败：' + error.message, 'error');
      });
    });

    $('#btn-run').addEventListener('click', function () {
      var tasks = getValidTasksOrAlert();
      if (!tasks) return;

      var taskSummary = tasks.map(function (task) {
        return '• ' + task.alias + ' / ' + task.table + ' / ' + task.opLabel;
      }).join('\n');

      // 真实流水线包含生产环境建表、配置上传和 SeaTunnel 启停，必须在操作时二次确认。
      var confirmed = window.confirm(
        '即将真实执行 run_pipeline.py。\n\n' +
        '该操作会修改测试及生产表结构，并可能停止、启动 SeaTunnel 任务。\n\n' +
        '本次共 ' + tasks.length + ' 条任务：\n' + taskSummary +
        '\n\n确认继续吗？'
      );
      if (!confirmed) return;

      setRunningUI(true);
      api.runPipeline($('#task-input').value).then(function (payload) {
        state.runId = payload.runId;
        state.submittedRunId = payload.runId;
        state.completedRunId = null;
        // 启动成功后清空待入湖任务输入框，避免已提交任务残留、被误重复提交。
        state.tasks = [];
        renderConfig();
        switchView('monitor');
        $('#log-panel').textContent = '真实流水线已提交，等待 run_pipeline.py 输出…';
        $('#log-meta').textContent = '真实流水线运行中';
        startPolling();
        return pollPipelineStatus(false);
      }).catch(function (error) {
        setRunningUI(false);
        notify('启动真实流水线失败：' + error.message, 'error');
      });
    });

    $('#btn-stop').addEventListener('click', function () {
      if (!state.runId || state.stopping) return;

      var confirmed = window.confirm(
        '确认手动停止当前入湖流水线吗？\n\n' +
        '系统将终止 run_pipeline.py 及其派生进程。已经完成的建表、配置上传或 SeaTunnel 操作不会自动回滚，任务文件将保留以便检查和重试。'
      );
      if (!confirmed) return;

      state.stopping = true;
      setRunningUI(true, true);
      switchView('monitor');
      $('#log-meta').textContent = '正在提交手动停止请求…';

      api.stopPipeline(state.runId).then(function () {
        startPolling();
        return pollPipelineStatus(false);
      }).catch(function (error) {
        notify('停止真实流水线失败：' + error.message, 'error');
        pollPipelineStatus(false).catch(function () {
          setRunningUI(state.running, false);
        });
      });
    });

    $('#btn-refresh').addEventListener('click', function () {
      pollPipelineStatus(true).catch(function () {
        // 错误已经在 pollPipelineStatus 中提示。
      });
    });

    $('#btn-history-refresh').addEventListener('click', function () {
      loadHistory(true).catch(function () {
        // 错误已经在 loadHistory 中提示。
      });
    });

    $('#btn-save-mapping').addEventListener('click', function () {
      var mapping = {
        system: $('#mapping-system').value,
        script: $('#mapping-script').value,
        clob: $('#mapping-clob').value
      };
      var clobErrors = getClobWhitelistErrors(mapping.clob);
      if (clobErrors.length) {
        notify('CLOB 白名单格式错误：\n' + clobErrors.join('\n'), 'error', 8000);
        return;
      }

      api.saveMapping(mapping).then(function () {
        state.mapping = mapping;
        notify('已写入正式系统映射、脚本映射和 CLOB 白名单文件。', 'success');
      }).catch(function (error) {
        notify('保存映射失败：' + error.message, 'error');
      });
    });

    $('#result-body').addEventListener('click', function (event) {
      var retryButton = event.target.closest('[data-retry]');
      if (retryButton) {
        retryFailedResult(retryButton.dataset.retry);
        return;
      }
      var detailButton = event.target.closest('[data-detail]');
      if (detailButton) {
        openDrawer(detailButton.dataset.detail);
      }
    });

    $('#history-body').addEventListener('click', function (event) {
      var button = event.target.closest('[data-history-detail]');
      if (button) {
        openHistoryDrawer(button.dataset.historyDetail);
      }
    });

    $('#btn-seatunnel-refresh').addEventListener('click', function () {
      loadSeatunnel(true).catch(function () {
        // 错误已经在 loadSeatunnel 中提示。
      });
    });

    $('#seatunnel-body').addEventListener('click', function (event) {
      var button = event.target.closest('[data-st-action]');
      if (!button || button.disabled) return;
      var action = button.dataset.stAction;
      var name = button.dataset.stName;
      if (action === 'config') {
        openSeatunnelConfig(name);
      } else if (action === 'log') {
        openSeatunnelStoredLog(name);
      } else {
        seatunnelAction(name, action);
      }
    });

    // ============ 数据重跑 ============
    $('#rerun-environments').addEventListener('change', function (event) {
      if (event.target.name !== 'rerun-environment' || state.rerun.running) return;
      var environmentId = event.target.value;
      if (event.target.checked) {
        if (state.rerun.selectedEnvironments.indexOf(environmentId) === -1) {
          state.rerun.selectedEnvironments.push(environmentId);
        }
      } else {
        state.rerun.selectedEnvironments = state.rerun.selectedEnvironments.filter(function (id) {
          return id !== environmentId;
        });
      }
      state.rerun.items = [];
      state.rerun.selected = {};
      state.rerun.errors = [];
      renderRerun();
    });

    $('#rerun-query').addEventListener('input', updateRerunControls);

    $('#rerun-search-form').addEventListener('submit', function (event) {
      event.preventDefault();
      searchRerunTables();
    });

    $('#rerun-result-body').addEventListener('change', function (event) {
      var checkbox = event.target;
      if (!checkbox.hasAttribute('data-rerun-result-index')) return;
      var item = state.rerun.items[Number(checkbox.dataset.rerunResultIndex)];
      if (!item) return;
      var key = rerunTableKey(item);
      if (checkbox.checked && item.oriExists) {
        state.rerun.selected[key] = {
          environment: item.environment,
          environmentLabel: item.environmentLabel,
          table: item.table,
          oriExists: true
        };
      } else {
        delete state.rerun.selected[key];
      }
      renderRerun();
    });

    $('#rerun-select-all').addEventListener('change', function (event) {
      var checked = event.target.checked;
      state.rerun.items.forEach(function (item) {
        var key = rerunTableKey(item);
        if (checked && item.oriExists) {
          state.rerun.selected[key] = {
            environment: item.environment,
            environmentLabel: item.environmentLabel,
            table: item.table,
            oriExists: true
          };
        } else {
          delete state.rerun.selected[key];
        }
      });
      renderRerun();
    });


    $('#rerun-selected-list').addEventListener('click', function (event) {
      var button = event.target.closest('[data-rerun-remove]');
      if (!button) return;
      delete state.rerun.selected[button.dataset.rerunRemove];
      renderRerun();
    });


    $('#btn-rerun-run').addEventListener('click', runRerun);
    $('#btn-rerun-log').addEventListener('click', openRerunLog);
    $('#rerun-history-query').addEventListener('input', function (event) {
      state.rerun.historyQuery = event.target.value;
      renderRerunHistory();
    });
    $('#btn-rerun-history-refresh').addEventListener('click', function () {
      loadRerunHistory(true);
    });
    $('#rerun-history-body').addEventListener('click', function (event) {
      var button = event.target.closest('[data-rerun-history]');
      if (button) openRerunHistory(button.dataset.rerunHistory);
    });

    $('#drawer-close').addEventListener('click', closeDrawer);
    $('#drawer-backdrop').addEventListener('click', closeDrawer);

    $('#log-modal-close').addEventListener('click', closeSeatunnelLogModal);
    $('#log-modal-backdrop').addEventListener('click', closeSeatunnelLogModal);
    $('#log-modal-fullscreen').addEventListener('click', toggleSeatunnelLogFullscreen);
    // 点击日志内容可切换全屏。
    $('#log-modal-content').addEventListener('click', toggleSeatunnelLogFullscreen);
  }

  // ============ 初始化 ============
  function init() {
    bindEvents();
    renderResults();
    renderHistory();
    renderCatalog();
    renderSourceManagement();
    renderSeatunnel();
    renderRerun();
    renderRerunHistory();
    switchView('config');

    // 先验证后端确实指向正式流水线，再读取正式配置。
    api.health().then(function (health) {
      var environmentBadge = document.querySelector('.env-badge');
      environmentBadge.textContent = '真实模式';
      environmentBadge.title = health.pipelineScript;
      loadCatalog();
      return Promise.all([
        api.fetchTasks(),
        api.fetchMapping(),
        api.fetchPipelineStatus(),
        api.fetchPipelineHistory()
      ]);
    }).then(function (responses) {
      var taskData = responses[0];
      var mappingData = responses[1];
      var pipelineData = responses[2];
      var historyData = responses[3];

      state.tasks = String(taskData.text || '')
        .split(/\r?\n/)
        .map(parseTaskLine)
        .filter(Boolean);
      state.mapping = mappingData;
      state.history = historyData.records || [];
      renderConfig();
      renderMapping();
      renderHistory();
      renderPipelineStatus(pipelineData);

      if (pipelineData.status === 'running' || pipelineData.status === 'stopping') {
        startPolling();
      }
    }).catch(function (error) {
      var environmentBadge = document.querySelector('.env-badge');
      environmentBadge.textContent = '后端未连接';
      environmentBadge.classList.add('env-error');
      state.catalog.available = false;
      state.catalog.loading = false;
      state.catalog.status = '后端未连接';
      renderCatalog();
      setRunningUI(false);
      $('#btn-run').disabled = true;
      notify(
        '未连接到真实入湖后端：' + error.message +
        '\n请运行 start_lake_ui.py 启动控制台。',
        'error',
        9000
      );
    });
  }

  window.addEventListener('beforeunload', function () {
    stopPolling();
    stopSeatunnelPolling();
    stopSeatunnelOperationPolling();
  });
  init();
})();

