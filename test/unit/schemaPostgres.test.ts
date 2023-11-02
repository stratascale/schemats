import * as assert from 'assert'
import * as sinon from 'sinon'
import * as proxyquire from 'proxyquire'
import * as PgPromise from 'pg-promise'
import { TableDefinition } from '../../src/schemaInterfaces'
import Options from '../../src/options'

const options = new Options({})
const pgp = PgPromise()

function tableDef(partial: { [name: string]: Omit<TableDefinition["columns"][0], "comment">}): TableDefinition {
    const columns: TableDefinition["columns"] = {};
    for (const key in partial) {
        columns[key] = {
            comment: "",
            ...partial[key],
        }
    }
    return {
        tableName: "",
        schemaName: "",
        columns,
        comment: "",
    }
}

describe('PostgresDatabase', () => {
    const sandbox = sinon.sandbox.create()
    const db = {
        query: sandbox.stub(),
        each: sandbox.stub(),
        map: sandbox.stub()
    }
    let PostgresDBReflection: any
    let PostgresProxy: any
    before(() => {
        const pgpStub: any = () => db
        pgpStub.as = pgp.as
        const SchemaPostgres = proxyquire('../../src/schemaPostgres', {
            'pg-promise': () => pgpStub
        })
        PostgresDBReflection = SchemaPostgres.PostgresDatabase
        PostgresProxy = new PostgresDBReflection()
    })
    beforeEach(() => {
        sandbox.reset()
    })
    after(() => {
        sandbox.restore()
    })
    describe('query', () => {
        it('calls postgres query', () => {
            PostgresProxy.query('SELECT * FROM TEST')
            assert.equal(db.query.getCall(0).args[0], 'SELECT * FROM TEST')
        })
    })
    describe('getEnumTypes', () => {
        it('writes correct query with schema name', () => {
            PostgresProxy.getEnumTypes('schemaName')
            assert.equal(db.each.getCall(0).args[0],
                'select n.nspname as schema, t.typname as name, e.enumlabel as value ' +
                'from pg_type t join pg_enum e on t.oid = e.enumtypid ' +
                'join pg_catalog.pg_namespace n ON n.oid = t.typnamespace ' +
                'where n.nspname = \'schemaName\' ' +
                'order by t.typname asc, e.enumlabel asc;')
            assert.deepEqual(db.each.getCall(0).args[1], [])
        })
        it('writes correct query without schema name', () => {
            PostgresProxy.getEnumTypes()
            assert.equal(db.each.getCall(0).args[0],
                'select n.nspname as schema, t.typname as name, e.enumlabel as value ' +
                'from pg_type t join pg_enum e on t.oid = e.enumtypid ' +
                'join pg_catalog.pg_namespace n ON n.oid = t.typnamespace  ' +
                'order by t.typname asc, e.enumlabel asc;')
            assert.deepEqual(db.each.getCall(0).args[1], [])
        })
        it('handles response from db', async () => {
            let enums = await PostgresProxy.getEnumTypes()
            const callback = db.each.getCall(0).args[2]
            const dbResponse = [
                {name: 'name', value: 'value1'},
                {name: 'name', value: 'value2'}
            ]
            dbResponse.forEach(callback)
            assert.deepEqual(enums, {name: ['value1', 'value2']})
        })
    })
    describe('loadTableColumns', () => {
        const td = { tableName: 'tableName', schemaName: 'schemaName', columns: {}, comment: ""};
        it('writes correct query', () => {
            PostgresProxy.loadTableColumns(td);
            assert.equal(db.each.getCall(0).args[0],
                'SELECT column_name, udt_name, is_nullable, column_default ' +
                'FROM information_schema.columns ' +
                'WHERE table_name = $1 and table_schema = $2')
            assert.deepEqual(db.each.getCall(0).args[1], [
                'tableName', 'schemaName'
            ])
        })
        it('handles response from db', async () => {
            let tableDefinition = await PostgresProxy.loadTableColumns(td);
            const callback = db.each.getCall(0).args[2]
            const dbResponse = [
                {column_name: 'col1', udt_name: 'int2', is_nullable: 'YES', column_default: null},
                {column_name: 'col2', udt_name: 'text', is_nullable: 'NO', column_default: null}
            ]
            dbResponse.forEach(callback)
            assert.deepEqual(tableDefinition, {
                ...td,
                columns: {
                    col1: { udtName: 'int2', nullable: true, defaultValue: null, comment: "" },
                    col2: { udtName: 'text', nullable: false, defaultValue: null, comment: "" }
                }
            })
        })
    })
    describe('getTableTypes', () => {
        const tableTypesSandbox = sinon.sandbox.create()
        before(() => {
            tableTypesSandbox.stub(PostgresProxy, 'getEnumTypes')
            tableTypesSandbox.stub(PostgresProxy, 'loadTableColumns')
            tableTypesSandbox.stub(PostgresDBReflection, 'mapTableDefinitionToType')
        })
        beforeEach(() => {
            tableTypesSandbox.reset()
        })
        after(() => {
            tableTypesSandbox.restore()
        })
        it('gets custom types from enums', async () => {
            PostgresProxy.getEnumTypes.returns(Promise.resolve({enum1: [], enum2: []}))
            PostgresProxy.loadTableColumns.returns(Promise.resolve({}))
            await PostgresProxy.getTableTypes('tableName', 'tableSchema')
            assert.deepEqual(PostgresDBReflection.mapTableDefinitionToType.getCall(0).args[1], ['enum1', 'enum2'])
        })
        it('gets table definitions', async () => {
            PostgresProxy.getEnumTypes.returns(Promise.resolve({}))
            PostgresProxy.loadTableColumns.returns(Promise.resolve({ table: {
                udtName: 'name',
                nullable: false
            }}))

            await PostgresProxy.getTableTypes({tableName: 'tableName', schemaName: 'tableSchema', columns: {}, comment: ""}, options)
            assert.deepEqual(
                PostgresProxy.loadTableColumns.getCall(0).args,
                [{
                    tableName: 'tableName', 
                    schemaName: 'tableSchema',
                    columns: {},
                    comment: "",
                }]
            )

            assert.deepEqual(PostgresDBReflection.mapTableDefinitionToType.getCall(0).args[0], { table: {
                udtName: 'name',
                nullable: false
            }})
        })
    })
    describe('getSchemaTables', () => {
        it('writes correct query', () => {
            PostgresProxy.getSchemaTables('schemaName')
            assert.equal(db.map.getCall(0).args[0],
                'SELECT table_name ' +
                'FROM information_schema.columns ' +
                'WHERE table_schema = $1 ' +
                'GROUP BY table_name')
            assert.deepEqual(db.map.getCall(0).args[1], ['schemaName'])
        })
        it('handles response from db', async () => {
            await PostgresProxy.getSchemaTables()
            const callback = db.map.getCall(0).args[2]
            const dbResponse = [
                {table_name: 'table1'},
                {table_name: 'table2'}
            ]
            const schemaTables = dbResponse.map(callback)
            assert.deepEqual(schemaTables, [
                {
                    columns: {},
                    comment: '',
                    schemaName: undefined,
                    tableName: 'table1'
                  },
                  {
                    columns: {},
                    comment: '',
                    schemaName: undefined,
                    tableName: 'table2'
                  }

            ])
        })
    })
    describe('mapTableDefinitionToType', () => {
        describe('maps to string', () => {
            it('bpchar', () => {
                const td = tableDef({
                    column: {
                        udtName: 'bpchar',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('char', () => {
                const td = tableDef({
                    column: {
                        udtName: 'char',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('varchar', () => {
                const td = tableDef({
                    column: {
                        udtName: 'varchar',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('text', () => {
                const td = tableDef({
                    column: {
                        udtName: 'text',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('citext', () => {
                const td = tableDef({
                    column: {
                        udtName: 'citext',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })            
            it('uuid', () => {
                const td = tableDef({
                    column: {
                        udtName: 'uuid',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('bytea', () => {
                const td = tableDef({
                    column: {
                        udtName: 'bytea',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('inet', () => {
                const td = tableDef({
                    column: {
                        udtName: 'inet',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('time', () => {
                const td = tableDef({
                    column: {
                        udtName: 'time',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('timetz', () => {
                const td = tableDef({
                    column: {
                        udtName: 'timetz',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('interval', () => {
                const td = tableDef({
                    column: {
                        udtName: 'interval',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('name', () => {
                const td = tableDef({
                    column: {
                        udtName: 'name',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
        })
        describe('maps to number', () => {
            it('int2', () => {
                const td = tableDef({
                    column: {
                        udtName: 'int2',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'number')
            })
            it('int4', () => {
                const td = tableDef({
                    column: {
                        udtName: 'int4',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'number')
            })
            it('int8', () => {
                const td = tableDef({
                    column: {
                        udtName: 'int8',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'string')
            })
            it('float4', () => {
                const td = tableDef({
                    column: {
                        udtName: 'float4',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'number')
            })
            it('float8', () => {
                const td = tableDef({
                    column: {
                        udtName: 'float8',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'number')
            })
            it('numeric', () => {
                const td = tableDef({
                    column: {
                        udtName: 'numeric',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'number')
            })
            it('money', () => {
                const td = tableDef({
                    column: {
                        udtName: 'money',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'number')
            })
            it('oid', () => {
                const td = tableDef({
                    column: {
                        udtName: 'oid',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'number')
            })
        })
        describe('maps to boolean', () => {
            it('bool', () => {
                const td = tableDef({
                    column: {
                        udtName: 'bool',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'boolean')
            })
        })
        describe('maps json to any', () => {
            it('json', () => {
                const td = tableDef({
                    column: {
                        udtName: 'json',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'any')
            })
            it('jsonb', () => {
                const td = tableDef({
                    column: {
                        udtName: 'jsonb',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'any')
            })
        })
        describe('maps to Date', () => {
            it('date', () => {
                const td = tableDef({
                    column: {
                        udtName: 'date',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Date')
            })
            it('timestamp', () => {
                const td = tableDef({
                    column: {
                        udtName: 'timestamp',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Date')
            })
            it('timestamptz', () => {
                const td = tableDef({
                    column: {
                        udtName: 'timestamptz',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Date')
            })
        })
        describe('maps to Array<number>', () => {
            it('_int2', () => {
                const td = tableDef({
                    column: {
                        udtName: '_int2',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Array<number>')
            })
            it('_int4', () => {
                const td = tableDef({
                    column: {
                        udtName: '_int4',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Array<number>')
            })
            it('_int8', () => {
                const td = tableDef({
                    column: {
                        udtName: '_int8',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Array<string>')
            })
            it('_float4', () => {
                const td = tableDef({
                    column: {
                        udtName: '_float4',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Array<number>')
            })
            it('_float8', () => {
                const td = tableDef({
                    column: {
                        udtName: '_float8',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Array<number>')
            })
            it('_numeric', () => {
                const td = tableDef({
                    column: {
                        udtName: '_numeric',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Array<number>')
            })
            it('_money', () => {
                const td = tableDef({
                    column: {
                        udtName: '_money',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Array<number>')
            })
        })
        describe('maps to Array<boolean>', () => {
            it('_bool', () => {
                const td = tableDef({
                    column: {
                        udtName: '_bool',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td, ['CustomType'], options).column.tsType, 'Array<boolean>')
            })
        })
        describe('maps to Array<string>', () => {
            it('_varchar', () => {
                const td = tableDef({
                    column: {
                        udtName: '_varchar',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td, ['CustomType'], options).column.tsType, 'Array<string>')
            })
            it('_text', () => {
                const td = tableDef({
                    column: {
                        udtName: '_text',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td, ['CustomType'], options).column.tsType, 'Array<string>')
            })
            it('_citext', () => {
                const td = tableDef({
                    column: {
                        udtName: '_citext',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td, ['CustomType'], options).column.tsType, 'Array<string>')
            })            
            it('_uuid', () => {
                const td = tableDef({
                    column: {
                        udtName: '_uuid',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td, ['CustomType'], options).column.tsType, 'Array<string>')
            })
            it('_bytea', () => {
                const td = tableDef({
                    column: {
                        udtName: '_bytea',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td, ['CustomType'], options).column.tsType, 'Array<string>')
            })
        })
        
        describe('maps to Array<Object>', () => {
            it('_json', () => {
                const td = tableDef({
                    column: {
                        udtName: '_json',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Array<Object>')
            })
            it('_jsonb', () => {
                const td = tableDef({
                    column: {
                        udtName: '_jsonb',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Array<Object>')
            })
        })
        
        describe('maps to Array<Date>', () => {
            it('_timestamptz', () => {
                const td = tableDef({
                    column: {
                        udtName: '_timestamptz',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td,[],options).column.tsType, 'Array<Date>')
            })
        })
        
        describe('maps to custom', () => {
            it('CustomType', () => {
                const td = tableDef({
                    column: {
                        udtName: 'CustomType',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td, ['CustomType'], options).column.tsType, 'CustomType')
            })
        })
        describe('maps to any', () => {
            it('UnknownType', () => {
                const td = tableDef({
                    column: {
                        udtName: 'UnknownType',
                        nullable: false,
                        defaultValue: null
                    }
                });
                assert.equal(PostgresDBReflection.mapTableDefinitionToType(td, ['CustomType'], options).column.tsType, 'any')
            })
        })
    })
})
