import _ from 'lodash'
import JSON5 from 'json5'
import Papa from 'papaparse'

const cc = DataStudioApp.createCommunityConnector()
const types = cc.FieldType
const aggregations = cc.AggregationType

function fetchCsv (request) {
  const url = _.get(request, 'configParams.url')
  Logger.log(url)
  const csv = _.trim(UrlFetchApp.fetch(url))
  return _.get(Papa.parse(csv, {
    encoding: 'utf8',
    header: true,
  }), 'data', [])
}

function getFieldsFromRequest (request) {
  const schemaConfig = JSON5.parse(_.get(request, 'configParams.schema', '[]'))
  const fields = cc.getFields()

  _.each(schemaConfig, field => {
    if (!field.id || !field.type) return
    fields[field.dimension ? 'newDimension' : 'newMetric']()
    fields.setId(field.id).setType(types[field.type])
    if (field.aggregation) fields.setAggregation(aggregations[field.aggregation])
  })

  return fields
}

function getAuthType () {
  const AuthTypes = cc.AuthType
  return cc.newAuthTypeResponse().setAuthType(AuthTypes.NONE).build()
}

function getConfig (request) {
  const config = cc.getConfig()

  config.newInfo().setId('instructions').setText('Enter npm package names to fetch their download count.')

  config
    .newTextInput()
    .setId('url')
    .setName('網址')
    .setHelpText('請輸入一個 CSV 的網址')
    .setPlaceholder('https://example.com/example.csv')

  config
    .newTextArea()
    .setId('schema')
    .setName('schema 陣列 (JSON)')
    .setHelpText('請以 JSON 格式輸入 schema 的陣列，可用的欄位有「id」、「type」、「dimension」、「aggregation」，詳細設定值請參考原始碼以及文件： https://developers.google.com/datastudio/connector/reference#field')

  return config.build()
}

function getSchema (request) {
  const fields = getFieldsFromRequest(request)

  return { schema: fields.build() }
}

function getData (request) {
  const fields = _.map(request.fields, 'name')
  return {
    schema: getFieldsFromRequest(request).forIds(fields).build(),
    rows: _.map(fetchCsv(request), row => _.pick(row, fields)),
  }
}

global.getAuthType = getAuthType
global.getConfig = getConfig
global.getSchema = getSchema
global.getData = getData
