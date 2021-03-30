import _ from 'lodash'
import JSON5 from 'json5'
import Papa from 'papaparse'

const cc = DataStudioApp.createCommunityConnector()
const types = cc.FieldType
const aggregations = cc.AggregationType

const ADMIN_EMAILS = [
  'taichunmin@gmail.com',
  'jimmy.dai@program.com.tw',
]

function showUserError (err) {
  cc.newUserError()
    .setDebugText(JSON.stringify(err.data))
    .setText(err.message)
    .throwException()
}

function fetchCsv (request) {
  const url = _.get(request, 'configParams.url')
  const csv = _.trim(UrlFetchApp.fetch(url).getContentText('UTF-8'))
  return _.get(Papa.parse(csv, {
    encoding: 'utf8',
    header: true,
  }), 'data', [])
}

function joiSchema (value) {
  // required
  _.each(['id', 'type'], k => {
    if (_.isNil(value[k])) throw new TypeError(`schema.${k} is required`)
  })
  // isString optional
  _.each(['aggregation', 'description', 'formula', 'group', 'id', 'name', 'type'], k => {
    if (!_.isNil(value[k]) && !_.isString(value[k])) throw new TypeError(`schema.${k} must be string`)
  })
  // isBoolean optional
  _.each(['default', 'hidden', 'metric', 'reaggregatable'], k => {
    if (!_.isNil(value[k]) && !_.isBoolean(value[k])) throw new TypeError(`schema.${k} must be boolean`)
  })
  // includes
  _.each([
    [_.keys(types), 'type'],
    [_.keys(aggregations), 'aggregation'],
  ], ([collection, k]) => {
    if (!_.isNil(value[k]) && !_.includes(collection, value[k])) throw new TypeError(`schema.${k} must be one of ${collection.join()}`)
  })
  return value
}

function parseJsonOrDefault (str, defaultValue) {
  try {
    if (!_.isString(str)) return defaultValue
    return JSON5.parse(str)
  } catch (err) {
    return defaultValue
  }
}

function getFieldsFromRequest (request) {
  const schemas = parseJsonOrDefault(_.get(request, 'configParams.schema'), [])
  const fields = cc.getFields()

  _.each(schemas, schema => {
    try {
      schema = joiSchema(schema)
      const newField = fields[`new${schema.metric ? 'Metric' : 'Dimension'}`]()
      if (_.isBoolean(schema.hidden)) newField.setIsHidden(schema.hidden)
      if (_.isBoolean(schema.reaggregatable)) newField.setIsReaggregatable(schema.reaggregatable)
      if (_.isString(schema.aggregation)) newField.setAggregation(aggregations[schema.aggregation])
      if (_.isString(schema.description)) newField.Description(schema.description)
      if (_.isString(schema.formula)) newField.setFormula(schema.formula)
      if (_.isString(schema.group)) newField.setGroup(schema.group)
      if (_.isString(schema.id)) newField.setId(schema.id)
      if (_.isString(schema.name)) newField.setName(schema.name)
      if (_.isString(schema.type)) newField.setType(types[schema.type])

      // default dimension/metric
      if (schema.default) fields[`setDefault${schema.metric ? 'Metric' : 'Dimension'}`](schema.id)
    } catch (err) {
      _.set(err, 'data.schema', schema)
      throw err
    }
  })

  return fields
}

function getAuthType () {
  const AuthTypes = cc.AuthType
  return cc.newAuthTypeResponse().setAuthType(AuthTypes.NONE).build()
}

function getConfig (request) {
  const config = cc.getConfig()

  config.newInfo().setId('instructions').setText('請輸入 CSV 網址以及資料格式來抓取外部資料。\nschema 陣列中每個物件可用的欄位有「id」、「type」、「metric」、「aggregation」，詳細設定值請參考原始碼以及文件： https://developers.google.com/datastudio/connector/reference#semantictype')

  config
    .newTextInput()
    .setId('url')
    .setName('網址')
    .setHelpText('請輸入一個 CSV 的網址')
    .setPlaceholder('https://example.com/example.csv')

  config
    .newTextArea()
    .setId('schema')
    .setName('schema 設定 (JSON5)')
    .setHelpText('請以 JSON5 格式輸入 schema 的陣列，可用的欄位有「id」、「type」、「metric」、「aggregation」，詳細設定值請參考原始碼以及文件： https://developers.google.com/datastudio/connector/reference#semantictype')

  return config.build()
}

function getSchema (request) {
  try {
    return { schema: getFieldsFromRequest(request).build() }
  } catch (err) {
    _.set(err, 'data.request', request)
    showUserError(err)
  }
}

function getData (request) {
  try {
    const fields = _.map(request.fields, 'name')
    const rows = _.chain(fetchCsv(request))
      .map(row => ({ values: _.values(_.pick(row, fields)) }))
      .value()
    return {
      schema: getFieldsFromRequest(request).forIds(fields).build(),
      rows,
    }
  } catch (err) {
    _.set(err, 'data.request', request)
    showUserError(err)
  }
}

function isAdminUser () {
  const email = Session.getEffectiveUser().getEmail()
  Logger.log(email)
  return _.includes(ADMIN_EMAILS, email)
}

global.getAuthType = getAuthType
global.getConfig = getConfig
global.getData = getData
global.getSchema = getSchema
global.isAdminUser = isAdminUser
