# Processor 组件

Processor 组件是数据流的处理环节，负责对数据进行过滤、转换、聚合等操作。

## 组件总览

| 组件名 | 目录 | 功能 | 并发支持 | 有状态 |
|--------|------|------|----------|--------|
| `processor-condition-filter` | processor/condition | 条件过滤 | ✅ (默认4，可通过 `concurrency` 配置) | ❌ |
| `processor-transform-field` | processor/transform | 字段转换/提取 | ✅ (默认4，可通过 `concurrency` 配置) | ❌ |
| `processor-expr-filter` | processor/expr | 表达式过滤 | ✅ (默认4，可通过 `concurrency` 配置) | ❌ |
| `processor-jq-transform` | processor/jqtransform | jq 表达式转换 | ✅ (默认4，可通过 `concurrency` 配置) | ❌ |
| `processor-python-script` | python/processor | Python 脚本处理 | ❌ | ✅ |

## 通用配置

所有 Processor 组件都实现 `dataflow.Processor` 接口：

```go
type Processor[T any] interface {
    Init(config []byte) error
    Process(ctx context.Context, in <-chan T, out chan<- T) error
}
```

---

## processor-condition-filter

### 作用

根据条件过滤记录，只保留满足条件的记录。支持多种比较操作符，类似 MongoDB 查询语法。

### 输入格式

`map[string]interface{}` 对象

### 输出格式

与输入格式相同，仅保留满足过滤条件的记录。

### 配置项

| 配置项 | 类型 | 必填 | 说明 |
|------|------|------|------|
| field | string | 是 | 要过滤的字段名 |
| op | string | 是 | 操作符 |
| value | interface | 是 | 比较值 |

### 支持的操作符

#### 比较操作符

| 操作符 | 说明 | 时间复杂度 | 示例 |
|------|------|-----------|------|
| eq | 等于 | O(1) | `op: eq, value: "active"` |
| ne | 不等于 | O(1) | `op: ne, value: "deleted"` |
| gt | 大于 | O(1) | `op: gt, value: 18` |
| gte | 大于等于 | O(1) | `op: gte, value: 60` |
| lt | 小于 | O(1) | `op: lt, value: 100` |
| lte | 小于等于 | O(1) | `op: lte, value: 1000` |

#### 数组操作符

| 操作符 | 说明 | 时间复杂度 | 示例 |
|------|------|-----------|------|
| in | 在数组中 | O(1)* | `op: in, value: [1, 2, 3]` |
| nin | 不在数组中 | O(1)* | `op: nin, value: ["a", "b"]` |

*使用 map 预处理优化

#### 存在性操作符

| 操作符 | 说明 | 时间复杂度 | 示例 |
|------|------|-----------|------|
| exists | 字段是否存在 | O(1) | `value: true` 表示存在，`false` 表示不存在 |

#### 字符串操作符

| 操作符 | 说明 | 时间复杂度 | 示例 |
|------|------|-----------|------|
| contains | 包含子串 | O(n) | `op: contains, value: "error"` |
| prefix | 前缀匹配 | O(m) | `op: prefix, value: "user-"` |
| suffix | 后缀匹配 | O(m) | `op: suffix, value: "@example.com"` |
| regex | 正则匹配 | O(?) | `op: regex, value: "^user-\\d+$"` |

### 类型转换

过滤组件会自动处理数值类型转换，支持以下场景：
- `int` 与 `float64` 互相比较（如 `1 == 1.0` 为 true）
- `json.Number` 与数值类型比较
- 不同整数类型比较（int32、int64 等）

### 示例

**过滤状态为 active 的记录：**
```yaml
processors:
  - name: processor-condition-filter
    config:
      field: status
      op: eq
      value: "active"
```

**过滤年龄大于 18 的记录：**
```yaml
processors:
  - name: processor-condition-filter
    config:
      field: age
      op: gt
      value: 18
```

**过滤分数在 60-100 之间（需多个处理器）：**
```yaml
processors:
  - name: processor-condition-filter
    config:
      field: score
      op: gte
      value: 60
  - name: processor-condition-filter
    config:
      field: score
      op: lte
      value: 100
```

**过滤状态为指定值之一的记录：**
```yaml
processors:
  - name: processor-condition-filter
    config:
      field: status
      op: in
      value: ["active", "pending", "processing"]
```

**过滤不以 "test-" 开头的记录：**
```yaml
processors:
  - name: processor-condition-filter
    config:
      field: id
      op: nin
      value: ["test-1", "test-2", "test-3"]
```

**过滤包含 "error" 的日志：**
```yaml
processors:
  - name: processor-condition-filter
    config:
      field: message
      op: contains
      value: "error"
```

**过滤 API 路径：**
```yaml
processors:
  - name: processor-condition-filter
    config:
      field: path
      op: prefix
      value: "/api/v1/"
```

**过滤特定邮箱域：**
```yaml
processors:
  - name: processor-condition-filter
    config:
      field: email
      op: suffix
      value: "@company.com"
```

**使用正则匹配用户 ID：**
```yaml
processors:
  - name: processor-condition-filter
    config:
      field: user_id
      op: regex
      value: "^user-\\d+$"
```

**过滤存在 email 字段的记录：**
```yaml
processors:
  - name: processor-condition-filter
    config:
      field: email
      op: exists
      value: true
```

### 性能建议

1. **优先使用 O(1) 操作符**：`eq`、`ne`、`gt`、`lt`、`in`、`nin` 等
2. **谨慎使用 `in`/`nin`**：虽然查找是 O(1)，但数组过大会增加初始化时间
3. **避免复杂正则**：`regex` 性能取决于正则复杂度，简单模式可用 `prefix`/`suffix`/`contains` 替代
4. **正则预编译**：正则表达式在初始化时编译并缓存，不会影响处理性能

### 并发特性

- **支持并发**: 是
- **建议最大并发数**: 4
- **有状态**: 否

---

## processor-transform-field

### 作用

转换记录的字段结构，支持字段提取、字段重命名、添加新字段、删除字段等操作。

### 输入格式

`map[string]interface{}` 对象

### 输出格式

转换后的 `map[string]interface{}` 对象。当使用 `extract_flatten: true` 时，可能输出多条记录。

### 配置项

| 配置项 | 类型 | 必填 | 说明 |
|------|------|------|------|
| extract | string | 否 | 提取指定字段的值作为新记录（支持点号分隔的嵌套路径） |
| extract_keep | array | 否 | 提取时保留的原始字段列表 |
| extract_flatten | bool | 否 | 当提取的字段是数组时，是否展开为多条记录（默认 false） |
| mapping | map | 否 | 字段映射，将旧字段名映射到新字段名 |
| add | map | 否 | 要添加的新字段及其值 |
| remove | array | 否 | 要删除的字段名列表 |

### 字段提取（extract）

`extract` 功能用于从嵌套字段中提取数据作为新的记录根。这在处理嵌套数据结构时非常有用。

**基本提取：**
```yaml
processors:
  - name: processor-transform-field
    config:
      extract: data
```
```json
// 输入: {"key":"243434","data":{"name":"xxx","list":[]}}
// 输出: {"name":"xxx","list":[]}
```

**提取嵌套字段：**
```yaml
processors:
  - name: processor-transform-field
    config:
      extract: payload.user.profile
```
```json
// 输入: {"event":"login","payload":{"user":{"profile":{"name":"Alice","age":30}}}}
// 输出: {"name":"Alice","age":30}
```

**提取并保留部分字段：**
```yaml
processors:
  - name: processor-transform-field
    config:
      extract: data
      extract_keep: [trace_id, timestamp]
```
```json
// 输入: {"trace_id":"abc","timestamp":"2024-01-01","data":{"name":"xxx"}}
// 输出: {"trace_id":"abc","timestamp":"2024-01-01","name":"xxx"}
```

**提取数组并展开：**
```yaml
processors:
  - name: processor-transform-field
    config:
      extract: items
      extract_flatten: true
      extract_keep: [order_id]
```
```json
// 输入: {"order_id":"O001","items":[{"sku":"A1","qty":2},{"sku":"B2","qty":1}]}
// 输出:
// {"order_id":"O001","sku":"A1","qty":2}
// {"order_id":"O001","sku":"B2","qty":1}
```

### 基本操作示例

**字段重命名：**
```yaml
processors:
  - name: processor-transform-field
    config:
      mapping:
        old_name: new_name
        user_id: userId
```

**添加新字段：**
```yaml
processors:
  - name: processor-transform-field
    config:
      add:
        processed_at: "2024-01-01"
        version: 1
```

**删除字段：**
```yaml
processors:
  - name: processor-transform-field
    config:
      remove:
        - internal_field
        - temp_data
```

**组合使用：**
```yaml
processors:
  - name: processor-transform-field
    config:
      extract: data
      extract_keep: [trace_id]
      mapping:
        name: user_name
        age: user_age
      add:
        source: "import"
      remove:
        - password
        - secret_key
```

### 执行顺序

1. 提取字段（extract）
2. 合并保留字段（extract_keep）
3. 展开数组（extract_flatten）
4. 应用字段映射（mapping）
5. 添加新字段（add）
6. 删除字段（remove）

### 边界情况

| 情况 | 处理方式 |
|------|---------|
| extract 字段不存在 | 输出空记录 `{}` |
| extract 字段是基本类型 | 输出空记录 `{}` |
| extract 数组为空 | 输出空记录 `{}` |
| extract_flatten 展开非对象元素 | 包装为 `{"_value": 元素值}` |

### 并发特性

- **支持并发**: 是
- **建议最大并发数**: 4
- **有状态**: 否

---

## processor-expr-filter

使用表达式语言进行灵活的数据过滤。支持复杂的逻辑表达式、比较运算、字符串操作和正则匹配。

相比 `processor-condition-filter`，表达式过滤器提供了更强大的灵活性：
- 支持复杂逻辑组合（AND、OR、NOT）
- 支持多条件组合在单个表达式中
- 支持嵌套字段访问
- 支持内置辅助函数

### 输入格式

`map[string]interface{}` 对象

### 输出格式

与输入格式相同，仅保留满足表达式的记录。

### 配置项

| 配置项 | 类型 | 必填 | 说明 |
|------|------|------|------|
| expression | string | 是 | 过滤表达式，返回 true 保留记录 |

### 支持的语法

#### 比较运算

| 运算符 | 说明 | 示例 |
|------|------|------|
| == | 等于 | `status == 'active'` |
| != | 不等于 | `status != 'deleted'` |
| > | 大于 | `score > 60` |
| >= | 大于等于 | `score >= 60` |
| < | 小于 | `age < 18` |
| <= | 小于等于 | `age <= 100` |
| in | 在数组中 | `status in ['active', 'pending']` |
| not in | 不在数组中 | `status not in ['deleted', 'archived']` |

#### 逻辑运算

| 运算符 | 说明 | 示例 |
|------|------|------|
| && | 逻辑与 | `status == 'active' && score > 60` |
| \|\| | 逻辑或 | `status == 'active' \|\| role == 'admin'` |
| ! | 逻辑非 | `!(status in ['deleted'])` |

#### 内置函数

| 函数 | 说明 | 示例 |
|------|------|------|
| str_contains(s, substr) | 字符串包含 | `str_contains(message, 'error')` |
| str_hasPrefix(s, prefix) | 前缀匹配 | `str_hasPrefix(path, '/api/')` |
| str_hasSuffix(s, suffix) | 后缀匹配 | `str_hasSuffix(email, '@company.com')` |
| str_matches(s, pattern) | 正则匹配 | `str_matches(user_id, '^user-\\d+$')` |

#### 字段检查

| 检查方式 | 说明 | 示例 |
|------|------|------|
| field != nil | 字段存在 | `email != nil` |
| field == nil | 字段不存在 | `deleted_at == nil` |

#### 嵌套字段

支持访问嵌套对象字段：

```
user.name == 'alice'
address.city == 'Beijing'
```

### 示例

**简单条件过滤：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "status == 'active'"
```

**多条件组合：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "status == 'active' && score >= 60"
```

**使用逻辑或：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "status == 'active' || role == 'admin'"
```

**使用 in 操作符：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "status in ['active', 'pending', 'processing']"
```

**字符串包含检查：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "str_contains(message, 'error')"
```

**前缀/后缀匹配：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "str_hasPrefix(path, '/api/') && str_hasSuffix(email, '@company.com')"
```

**正则匹配：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "str_matches(user_id, '^user-\\d+$')"
```

**字段存在性检查：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "email != nil && deleted_at == nil"
```

**复杂表达式：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "(status == 'active' || role == 'admin') && score > 50 && str_contains(email, '@')"
```

**范围检查：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "score >= 60 && score <= 100"
```

**嵌套字段访问：**
```yaml
processors:
  - name: processor-expr-filter
    config:
      expression: "user.role == 'admin' && user.department == 'engineering'"
```

### 与 processor-condition-filter 的对比

| 特性 | processor-condition-filter | processor-expr-filter |
|------|---------------------------|----------------------|
| 单条件过滤 | ✅ 简单配置 | ✅ 表达式 |
| 多条件组合 | 需要多个处理器 | ✅ 单个表达式 |
| 复杂逻辑 | ❌ | ✅ AND/OR/NOT |
| 嵌套字段 | ❌ | ✅ |
| 性能 | 更优（预编译操作符） | 良好（表达式编译） |
| 灵活性 | 中等 | 高 |

### 性能建议

1. **简单条件使用 condition-filter**：单个字段的简单比较，使用 `processor-condition-filter` 更高效
2. **复杂条件使用 expr-filter**：需要多条件组合时，单个 `processor-expr-filter` 比多个 `processor-condition-filter` 更高效
3. **避免复杂正则**：正则匹配性能取决于模式复杂度
4. **预编译优化**：表达式在初始化时编译，运行时无编译开销

### 并发特性

- **支持并发**: 是
- **建议最大并发数**: 4
- **有状态**: 否

---

## processor-jq-transform

### 作用

使用 **jq 语法** 进行强大的数据转换。

jq 是一个功能强大的 JSON 处理语言，相比其他转换组件：
- `processor-transform-field`：简单的字段映射/提取
- `processor-jq-transform`：完整的 jq 语法
- 支持复杂的数组操作、条件判断、计算字段、数据重组等

### 输入格式

`map[string]interface{}` 对象

### 输出格式

转换后的 `map[string]interface{}` 对象。当使用数组展开（如 `.items[]`）时，可能输出多条记录。

### 配置项

| 配置项 | 类型 | 必填 | 说明 |
|------|------|------|------|
| query | string | 是 | jq 查询表达式 |

### jq 语法快速参考

#### 基本操作

| 语法 | 说明 | 示例 |
|------|------|------|
| `.` | 当前记录 | `.` |
| `.field` | 访问字段 | `.name` |
| `.nested.field` | 嵌套访问 | `.user.profile.name` |
| `{a: .x, b: .y}` | 构造对象 | `{user_id: .id, name}` |
| `.items[]` | 展开数组 | 展开为多条记录 |

#### 数组操作

| 语法 | 说明 | 示例 |
|------|------|------|
| `map(expr)` | 映射 | `.items \| map(.price)` |
| `add` | 求和 | `.prices \| add` |
| `length` | 长度 | `.items \| length` |
| `first`, `last` | 首尾元素 | `.items \| first` |
| `sort`, `reverse` | 排序/反转 | `.items \| sort` |

#### 条件与过滤

| 语法 | 说明 | 示例 |
|------|------|------|
| `if-then-else` | 条件 | `if .score > 60 then "pass" else "fail" end` |
| `select(cond)` | 过滤 | `.items[] \| select(.price > 100)` |
| `empty` | 空值（过滤） | `if .active then . else empty end` |

#### 变量绑定

| 语法 | 说明 | 示例 |
|------|------|------|
| `. as $var` | 绑定变量 | `. as $order \| .items[]` |
| `$var` | 引用变量 | 访问之前绑定的值 |

### 示例

**字段选择和重命名：**
```yaml
processors:
  - name: processor-jq-transform
    config:
      query: "{user_id: .id, user_name: .name, email: .contact.email}"
```
```json
// 输入: {"id": 1, "name": "Alice", "contact": {"email": "alice@example.com"}, "secret": "..."}
// 输出: {"user_id": 1, "user_name": "Alice", "email": "alice@example.com"}
```

**展开数组并保留父级字段：**
```yaml
processors:
  - name: processor-jq-transform
    config:
      # 使用变量绑定保留父级数据
      query: |
        . as $order |
        .items[] |
        {
          order_id: $order.order_id,
          customer: $order.customer.name,
          sku,
          subtotal: (.price * .qty)
        }
```

**计算聚合字段：**
```yaml
processors:
  - name: processor-jq-transform
    config:
      query: |
        {
          order_id,
          total: (.items | map(.price) | add),
          item_count: (.items | length),
          avg_price: ((.items | map(.price) | add) / (.items | length))
        }
```

**条件转换：**
```yaml
processors:
  - name: processor-jq-transform
    config:
      query: |
        if .score >= 90 then
          {name, grade: "A", status: "excellent"}
        elif .score >= 60 then
          {name, grade: "B", status: "pass"}
        else
          {name, grade: "F", status: "fail"}
        end
```

**过滤记录：**
```yaml
processors:
  - name: processor-jq-transform
    config:
      # 只保留活跃用户，不活跃的返回 empty（跳过）
      query: "if .active then {id, name, email} else empty end"
```

### 边界情况

| 情况 | 处理方式 |
|------|---------|
| 字段不存在 | 返回 `null` |
| 表达式错误 | 跳过该记录 |
| 返回 `empty` | 跳过该记录（用于过滤） |
| 返回数组 | 展开为多条记录 |
| 返回基本类型 | 包装为 `{"_value": 结果}` |
| 返回 `null` | 跳过该记录 |

### 与其他组件对比

| 特性 | processor-transform-field | processor-jq-transform |
|------|---------------------------|---------------------------|
| 字段映射/重命名 | ✅ | ✅ |
| 字段提取 | ✅ `extract` | ✅ `.field` |
| 数组展开 | ✅ `extract_flatten` | ✅ `.items[]` |
| 条件转换 | ❌ | ✅ `if-then-else` |
| 计算字段 | ❌ | ✅ 算术运算 |
| 复杂数据重组 | ❌ | ✅ |
| 学习成本 | 低 | 中等（jq 语法） |
| 灵活性 | 中等 | 非常高 |
| 性能 | 更优 | 良好 |

### 性能建议

1. **简单转换用 transform-field**：简单的字段映射、添加、删除，使用 `processor-transform-field` 更高效
2. **复杂转换用 jq-transform**：需要数组操作、计算、条件判断时使用
3. **避免过度复杂的表达式**：jq 表达式在初始化时编译，但复杂表达式仍会影响性能
4. **善用变量绑定**：在展开数组时使用 `. as $var` 保留父级数据

### 并发特性

- **支持并发**: 是
- **建议最大并发数**: 4
- **有状态**: 否

### jq 参考资源

- **官方手册**: https://stedolan.github.io/jq/manual/
- **在线练习**: https://jqplay.org/
- **GitHub**: https://github.com/stedolan/jq
- **gojq 文档**: https://github.com/itchyny/gojq
