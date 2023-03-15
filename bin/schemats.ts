#! /usr/bin/env node
/**
 * Commandline interface
 * Created by xiamx on 2016-08-10.
 */

import * as yargs from 'yargs'
import * as fs from 'fs'
import { typescriptOfSchema } from '../src/index'

interface SchematsConfig {
    conn: string
    table: string[] | string
    schema: string
    output: string
    camelCase?: boolean
    noHeader?: boolean
    tableNamespaces?: boolean
    forInsert?: boolean
    sqlite3?: boolean
    skipTables?: string[] | string
    skipPrefix?: string[] | string
    customTypes?: object
    customHeader?: string
    prettier?: boolean
    prettierConfig?: any
}

let argv: SchematsConfig = yargs
    .usage('Usage: $0 <command> [options]')
    .global('config')
    .default('config', 'schemats.json')
    .config()
    .env('SCHEMATS')
    .command('generate', 'generate type definition')
    .demand(1)
    // tslint:disable-next-line
    .example(
        '$0 generate -c postgres://username:password@localhost/db -t table1 -t table2 -s schema -o interface_output.ts',
        'generate typescript interfaces from schema'
    )
    .demand('c')
    .alias('c', 'conn')
    .nargs('c', 1)
    .describe('c', 'database connection string')
    .alias('t', 'table')
    .nargs('t', 1)
    .describe('t', 'table name')
    .alias('s', 'schema')
    .nargs('s', 1)
    .describe('s', 'schema name')
    .alias('n', 'tableNamespaces')
    .describe('n', 'print table namspaces')
    .alias('i', 'forInsert')
    .describe('i', 'for insert (optional on cols with defaults)')
    .alias('C', 'camelCase')
    .describe('C', 'Camel-case columns')
    .describe('noHeader', 'Do not write header')
    .demand('o')
    .nargs('o', 1)
    .alias('o', 'output')
    .describe('o', 'output file name')
    .describe('sqlite3', 'For sqlite3 dbs')
    .describe('skipTables', 'tables to skip')
    .describe('skipPrefix', 'tables to skip by their prefix')
    .describe('customTypes', 'Mapping of custom types for a table column')
    .describe('customHeader', 'Custom header to prefix the output file')
    .help('h')
    .alias('h', 'help').argv
;(async () => {
    try {
        if (!Array.isArray(argv.table)) {
            if (!argv.table) {
                argv.table = []
            } else {
                argv.table = [argv.table]
            }
        }

        let formattedOutput = await typescriptOfSchema(
            argv.conn,
            argv.table,
            argv.schema,
            {
                prettier: argv.prettier,
                prettierConfig: argv.prettierConfig,
                customHeader: argv.customHeader,
                customTypes: argv.customTypes,
                camelCase: argv.camelCase,
                writeHeader: !argv.noHeader,
                tableNamespaces: Boolean(argv.tableNamespaces),
                forInsert: Boolean(argv.forInsert),
                sqlite3: Boolean(argv.sqlite3),
                skipTables:
                    typeof argv.skipTables === 'string'
                        ? [argv.skipTables]
                        : argv.skipTables,
                skipPrefix:
                    typeof argv.skipPrefix === 'string'
                        ? [argv.skipPrefix]
                        : argv.skipPrefix,
            }
        )
        fs.writeFileSync(argv.output, formattedOutput)
    } catch (e) {
        console.error(e)
        process.exit(1)
    }
})()
    .then(() => {
        process.exit()
    })
    .catch((e: any) => {
        console.warn(e)
        process.exit(1)
    })
