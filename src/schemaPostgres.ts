import type * as PgPromise from 'pg-promise'
import { transform } from 'lodash'
import { keys } from 'lodash'
import Options from './options'

import { TableDefinition, Database } from './schemaInterfaces'

export class PostgresDatabase implements Database {
    readonly db: PgPromise.IDatabase<{}>
    readonly pgp: PgPromise.IMain

    constructor(public connectionString: string) {
        let PgPromise: typeof import('pg-promise')
        try {
            PgPromise = require('pg-promise') as typeof import('pg-promise')
        } catch (e) {
            throw new Error(
                'pg-promise is required as a peerDependency of @tgriesser/schemats'
            )
        }
        this.pgp = PgPromise()
        this.db = this.pgp(connectionString)
    }

    private static mapTableDefinitionToType(
        table: TableDefinition,
        customTypes: string[],
        options: Options,
    ): TableDefinition["columns"] {
        return transform(table.columns, (acc, column, columnName) => {
            acc[columnName] = column
            if (
                options.options.customTypes?.[table.tableName]?.[columnName] !==
                undefined
            ) {
                column.tsCustomType = true
                column.tsType =
                    options.options.customTypes[table.tableName][columnName]
                return
            }

            if (
                options.options.customTypeTransform?.[column.udtName] !==
                undefined
            ) {
                column.tsType =
                    options.options.customTypeTransform?.[column.udtName]
                return
            }

            // console.log(column, columnName)
            switch (column.udtName) {
                case 'bpchar':
                case 'char':
                case 'varchar':
                case 'text':
                case 'citext':
                case 'uuid':
                case 'bytea':
                case 'inet':
                case 'time':
                case 'timetz':
                case 'interval':
                case 'name':
                    column.tsType = 'string'
                    break
                case 'int8': // BigInt is cast as string in pg
                    column.tsType = 'string'
                    break
                case 'int2':
                case 'int4':
                case 'float8':
                case 'float4':
                case 'numeric':
                case 'money':
                case 'oid':
                    column.tsType = 'number'
                    break
                case 'bool':
                    column.tsType = 'boolean'
                    break
                case 'json':
                case 'jsonb':
                    column.tsType = 'any'
                    break
                case 'date':
                case 'timestamp':
                case 'timestamptz':
                    column.tsType = 'Date'
                    break
                case '_int8':
                    column.tsType = 'Array<string>'
                    break
                case '_int2':
                case '_int4':
                case '_float4':
                case '_float8':
                case '_numeric':
                case '_money':
                    column.tsType = 'Array<number>'
                    break
                case '_bool':
                    column.tsType = 'Array<boolean>'
                    break
                case '_varchar':
                case '_text':
                case '_citext':
                case '_uuid':
                case '_bytea':
                    column.tsType = 'Array<string>'
                    break
                case '_json':
                case '_jsonb':
                    column.tsType = 'Array<Object>'
                    break
                case '_timestamptz':
                    column.tsType = 'Array<Date>'
                    break
                default:
                    if (customTypes.indexOf(column.udtName) !== -1) {
                        column.tsType = options.transformTypeName(
                            column.udtName
                        )
                    } else {
                        console.log(
                            `Type [${column.udtName} has been mapped to [any] because no specific type has been found.`
                        )
                        column.tsType = 'any'
                    }
            }
        });
    }

    public query(queryString: string) {
        return this.db.query(queryString)
    }

    public async getEnumTypes(schema?: string) {
        type T = { name: string; value: any }
        let enums: any = {}
        let enumSchemaWhereClause = schema
            ? this.pgp.as.format(`where n.nspname = $1`, schema)
            : ''
        await this.db.each<T>(
            'select n.nspname as schema, t.typname as name, e.enumlabel as value ' +
                'from pg_type t ' +
                'join pg_enum e on t.oid = e.enumtypid ' +
                'join pg_catalog.pg_namespace n ON n.oid = t.typnamespace ' +
                `${enumSchemaWhereClause} ` +
                'order by t.typname asc, e.enumlabel asc;',
            [],
            (item: T) => {
                if (!enums[item.name]) {
                    enums[item.name] = []
                }
                enums[item.name].push(item.value)
            }
        )
        return enums
    }

    public async loadTableColumns(table: TableDefinition) {
        let tableDefinition: TableDefinition = { ...table }
        type T = {
            column_name: string
            udt_name: string
            is_nullable: string
            column_default: string | null
        }
        await this.db.each<T>(
            'SELECT column_name, udt_name, is_nullable, column_default ' +
                'FROM information_schema.columns ' +
                'WHERE table_name = $1 and table_schema = $2',
            [table.tableName, table.schemaName],
            (schemaItem: T) => {
                tableDefinition.columns[schemaItem.column_name] = {
                    udtName: schemaItem.udt_name,
                    nullable: schemaItem.is_nullable === 'YES',
                    defaultValue: schemaItem.column_default,
                    comment: "",
                }
            }
        )
        return tableDefinition
    }

    public async getTableTypes(
        table: TableDefinition,
        options: Options
    ) {
        let enumTypes = await this.getEnumTypes()
        let customTypes = keys(enumTypes)
        return PostgresDatabase.mapTableDefinitionToType(
            await this.loadTableColumns(table),
            customTypes.sort(),
            options,
        )
    }

    public async getSchemaTables(schemaName: string): Promise<TableDefinition[]> {
        const schemaTables = await this.db.map<TableDefinition>(
            'SELECT table_name ' +
                'FROM information_schema.columns ' +
                'WHERE table_schema = $1 ' +
                'GROUP BY table_name',
            [schemaName],
            (schemaItem: { table_name: string }) => {
                return {
                    schemaName,
                    tableName: schemaItem.table_name,
                    comment: "",
                    columns: {},
                };
            }
        )

        return schemaTables?.sort()
    }

    getDefaultSchema(): string {
        return 'public'
    }
}
