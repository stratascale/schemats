/**
 * Schemats takes sql database schema and creates corresponding typescript definitions
 * Created by xiamx on 2016-08-10.
 */

import {
    generateEnumType,
    generateTableTypes,
    generateTableInterface,
    generateTableInterfaceOnly,
    generateEnumManifest,
    generateTableManifest,
} from './typescript'
import { getDatabase, Database } from './schema'
import Options, { OptionValues } from './options'
import { TableDefinition } from './schemaInterfaces'
const pkgVersion = require('../package.json').version

function getTime() {
    let padTime = (value: number) => `0${value}`.slice(-2)
    let time = new Date()
    const yyyy = time.getFullYear()
    const MM = padTime(time.getMonth() + 1)
    const dd = padTime(time.getDate())
    const hh = padTime(time.getHours())
    const mm = padTime(time.getMinutes())
    const ss = padTime(time.getSeconds())
    return `${yyyy}-${MM}-${dd} ${hh}:${mm}:${ss}`
}

function buildHeader(
    db: Database,
    tables: string[],
    schema: string | null,
    options: OptionValues
): string {
    let commands = [
        'schemats',
        'generate',
        '-c',
        db.connectionString.replace(/:\/\/.*@/, '://username:password@'),
    ]
    if (options.camelCase) commands.push('-C')
    if (tables.length > 0) {
        tables.forEach((t: string) => {
            commands.push('-t', t)
        })
    }
    if (schema) {
        commands.push('-s', schema)
    }

    return `
        /**
         * AUTO-GENERATED FILE - DO NOT EDIT!
         *
         * This file was automatically generated by schemats v.${pkgVersion}
         *
         */

    `
}

export async function typescriptOfTable(
    db: Database | string,
    table: string,
    schema: string,
    optionsObject: Options
): Promise<string> {
    if (typeof db === 'string') {
        db = getDatabase(db, optionsObject.options)
    }

    let tableDef: TableDefinition = {
        tableName: table,
        schemaName: schema,
        columns: {},
        comment: ""
    }
    let interfaces = ''
    tableDef.columns = await db.getTableTypes(tableDef, optionsObject)

    if (optionsObject.options.tableNamespaces) {
        interfaces += generateTableTypes(tableDef, optionsObject)
        interfaces += generateTableInterface(tableDef, optionsObject)
    } else {
        interfaces += generateTableInterfaceOnly(
            tableDef,
            optionsObject
        )
    }
    return interfaces
}

export async function typescriptOfSchema(
    db: Database | string,
    tables: string[] = [],
    schema: string | null = null,
    options: OptionValues = {}
): Promise<string> {
    if (typeof db === 'string') {
        db = getDatabase(db, options)
    }

    if (!schema) {
        schema = db.getDefaultSchema()
    }

    const tableDefs = [] as TableDefinition[];
    if (tables.length === 0) {
        tableDefs.push(...await db.getSchemaTables(schema));
    } else {
        tableDefs.push(...tables.map((name): TableDefinition => {
            return {
                tableName: name,
                schemaName: schema ?? "",
                columns: {},
                comment: "",
            }
        }));
    }

    if (options.skipTables?.length) {
        tables = tables.filter((t) => !options.skipTables?.includes(t))
    }

    if (options.skipPrefix?.length) {
        tables = tables.filter(
            (t) => !options.skipPrefix?.some((prefix) => t.startsWith(prefix))
        )
    }

    const optionsObject = new Options(options)

    const enums = await db.getEnumTypes(schema)
    const enumTypes = generateEnumType(enums, optionsObject)
    const interfacePromises = tables.map((table) =>
        typescriptOfTable(db, table, schema as string, optionsObject)
    )
    const interfaces = await Promise.all(interfacePromises).then((tsOfTable) =>
        tsOfTable.join('')
    )

    let output = ''
    if (optionsObject.options.customHeader) {
        output += optionsObject.options.customHeader
        output += '\n'
    } else {
        output += '/* tslint:disable */\n\n'
        if (optionsObject.options.writeHeader) {
            output += buildHeader(db, tables, schema, options)
        }
    }
    output += enumTypes
    output += interfaces

    if (optionsObject.options.tableManifest) {
        output += generateTableManifest(tables, optionsObject)
    }

    if (optionsObject.options.enumManifest && enums.length) {
        output += generateEnumManifest(enums, optionsObject)
    }

    if (optionsObject.options.customFooter) {
        output += optionsObject.options.customFooter
    }

    if (optionsObject.options.prettier) {
        try {
            const [prettier, parserTs] = await Promise.all([
                import('prettier/standalone'),
                import('prettier/parser-typescript'),
            ])
            return prettier.format(output, {
                parser: 'typescript',
                plugins: [parserTs],
                ...(optionsObject.options.prettierConfig || {}),
            })
        } catch (e) {
            throw new Error(
                'Install prettier as a devDependency, or pass prettier:false to the schemats options'
            )
        }
    }

    return output
}

export { Database, getDatabase } from './schema'
export { Options, OptionValues }
