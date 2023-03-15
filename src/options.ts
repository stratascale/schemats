import { camelCase, upperFirst } from 'lodash'
import { ParserOptions } from 'prettier'

const DEFAULT_OPTIONS: OptionValues = {
    prettier: true,
    writeHeader: true,
    camelCase: false,
    tableNamespaces: false,
    enumManifest: false,
    tableManifest: true,
    forInsert: false,
    forInsertNull: false,
    // inlineEnum: false
}

export type OptionValues = {
    sqlite3?: boolean
    prettier?: boolean
    prettierConfig?: ParserOptions
    camelCase?: boolean
    writeHeader?: boolean // write schemats description header
    customHeader?: string
    customFooter?: string
    tableNamespaces?: boolean // whether to namespace field types
    tableManifest?: boolean | string // whether to include a lookup of all tables, if a string is provided, it's the name of the interface
    enumManifest?: boolean | string // whether to include a lookup of all enums, if a string is provided, it's the name of the interface
    forInsert?: boolean // makes any columns with a "default" optional
    forInsertNull?: boolean // whether to require nullable columns without a default
    customTypes?: Record<string, any>
    customTypeTransform?: Record<string, any> // transform the native type to a ts type
    skipTables?: string[]
    skipPrefix?: string[]
    // inlineEnum?: boolean // whether to create/export enum types
}

export default class Options {
    public options: OptionValues

    constructor(options: OptionValues = {}) {
        this.options = { ...DEFAULT_OPTIONS, ...options }
    }

    transformTypeName(typename: string) {
        return this.options.camelCase
            ? upperFirst(camelCase(typename))
            : typename
    }

    transformColumnName(columnName: string) {
        return this.options.camelCase ? camelCase(columnName) : columnName
    }
}
