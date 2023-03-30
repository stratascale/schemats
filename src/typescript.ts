/**
 * Generate typescript interface from table schema
 * Created by xiamx on 2016-08-10.
 */

import * as _ from 'lodash'

import { TableDefinition, ColumnDefinition } from './schemaInterfaces'
import Options from './options'

function nameIsReservedKeyword(name: string): boolean {
    const reservedKeywords = ['string', 'number', 'package']
    return reservedKeywords.indexOf(name) !== -1
}

function normalizeName(name: string, options: Options): string {
    if (nameIsReservedKeyword(name)) {
        return name + '_'
    } else {
        return name
    }
}

function colon(def: ColumnDefinition, options: Options) {
    if (options.options.forInsert !== true) {
        return ':'
    }
    if (typeof def.defaultValue === 'string') {
        return '?:'
    }
    if (def.nullable && options.options.forInsertNull !== true) {
        return '?:'
    }
    return ':'
}

export function generateTableInterface(
    tableNameRaw: string,
    tableDefinition: TableDefinition,
    options: Options
) {
    const tableName = options.transformTypeName(tableNameRaw)
    let members = ''
    Object.keys(tableDefinition)
        .sort()
        .forEach((c) => {
            const d = tableDefinition[c]
            const columnName = options.transformColumnName(c)
            members += `${columnName}${colon(
                d,
                options
            )} ${tableName}Fields.${normalizeName(columnName, options)};\n`
        })

    return `
        export interface ${normalizeName(tableName, options)} {
        ${members}
        }
    `
}

export function generateTableInterfaceOnly(
    tableNameRaw: string,
    tableDefinition: TableDefinition,
    options: Options
) {
    const tableName = options.transformTypeName(tableNameRaw)
    let members = ''
    Object.keys(tableDefinition)
        .sort()
        .forEach((columnNameRaw) => {
            const def = tableDefinition[columnNameRaw]
            const type = def.tsType
            const nullable = def.nullable && !def.tsCustomType ? '| null' : ''
            const columnName = options.transformColumnName(columnNameRaw)
            members += `${columnName}${colon(
                def,
                options
            )}${type}${nullable};\n`
        })

    return `
        export interface ${normalizeName(tableName, options)} {
        ${members}
        }
    `
}

export function generateEnumManifest(enumObject: any, options: Options) {
    const name =
        typeof options.options.enumManifest === 'string'
            ? options.options.enumManifest
            : 'DBEnums'
    let enumManifest = [`\nexport interface ${name} {`]
    for (let enumNameRaw in enumObject) {
        enumManifest.push(
            `  ${options.transformTypeName(
                enumNameRaw
            )} :${options.transformTypeName(enumNameRaw)}`
        )
    }
    return enumManifest.concat('}\n').join('\n')
}

export function generateTableManifest(tables: string[], optionsObj: Options) {
    const { options } = optionsObj
    const typeName =
        typeof options.tableManifest === 'string'
            ? options.tableManifest
            : 'DBTables'

    return [
        `\nexport interface ${typeName} {`,
        ...tables.map(
            (t) =>
                `  ${optionsObj.transformColumnName(t)}: ${normalizeName(
                    optionsObj.transformTypeName(t),
                    optionsObj
                )}`
        ),
        `}\n`,
    ].join('\n')
}

export function generateEnumType(enumObject: any, options: Options) {
    let enumString = ''
    for (let enumNameRaw in enumObject) {
        const enumName = options.transformTypeName(enumNameRaw)
        enumString += `export type ${enumName} = `
        enumString += enumObject[enumNameRaw]
            .map((v: string) => `'${v}'`)
            .join(' | ')
        enumString += ';\n'
    }
    return enumString
}

export function generateTableTypes(
    tableNameRaw: string,
    tableDefinition: TableDefinition,
    options: Options
) {
    const tableName = options.transformTypeName(tableNameRaw)
    let fields = ''
    Object.keys(tableDefinition)
        .sort()
        .forEach((columnNameRaw) => {
            let type = tableDefinition[columnNameRaw].tsType
            let nullable =
                tableDefinition[columnNameRaw].nullable &&
                !tableDefinition[columnNameRaw].tsCustomType
                    ? '| null'
                    : ''
            const columnName = options.transformColumnName(columnNameRaw)
            fields += `export type ${normalizeName(
                columnName,
                options
            )} = ${type}${nullable};\n`
        })

    return `
        export namespace ${tableName}Fields {
        ${fields}
        }
    `
}
