import _ from 'lodash'
import JSON5 from 'json5'
import Papa from 'papaparse'

const cc = DataStudioApp.createCommunityConnector()
const types = cc.FieldType
const aggregations = cc.AggregationType

function fetchCsv (request) {
  const url = _.get(request, 'configParams.url')
  Logger.log(url)
  const csv = _.trim(UrlFetchApp.fetch(url).getContentText('UTF-8'))
  return _.get(Papa.parse(csv, {
    encoding: 'utf8',
    header: true,
  }), 'data', [])
}

function getFieldsFromRequest (request) {
  const schemaConfig = JSON5.parse(_.get(request, 'configParams.schema', '[]'))
  const fields = cc.getFields()

  _.each(schemaConfig, field => {
    try {
      if (!field.id || !field.type) return
      const newField = fields[field.metric ? 'newMetric' : 'newDimension']()
      newField.setId(field.id).setType(types[field.type])
      if (field.aggregation) newField.setAggregation(aggregations[field.aggregation])
    } catch (err) {
      err.message = `${err.message} ${JSON.stringify(field)}`
      throw err
    }
  })

  return fields
}

function getAuthType () {
  Logger.log('getAuthType')
  const AuthTypes = cc.AuthType
  return cc.newAuthTypeResponse().setAuthType(AuthTypes.NONE).build()
}

function getConfig (request) {
  Logger.log('getConfig')
  const config = cc.getConfig()

  config.newInfo().setId('instructions').setText('請輸入 CSV 網址以及資料格式來抓取外部資料。')

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
    .setHelpText('請以 JSON 格式輸入 schema 的陣列，可用的欄位有「id」、「type」、「metric」、「aggregation」，詳細設定值請參考原始碼以及文件： https://developers.google.com/datastudio/connector/reference#field')

  return config.build()
}

function getSchema (request) {
  Logger.log('getSchema')
  const fields = getFieldsFromRequest(request)

  return { schema: fields.build() }
}

function getData (request) {
  Logger.log('getData')
  const fields = _.map(request.fields, 'name')
  const rows = _.chain(fetchCsv(request))
    .map(row => ({ values: _.values(_.pick(row, fields)) }))
    .value()
  Logger.log(_.first(rows))
  return {
    schema: getFieldsFromRequest(request).forIds(fields).build(),
    rows,
  }
}

global.getAuthType = getAuthType
global.getConfig = getConfig
global.getSchema = getSchema
global.getData = getData
