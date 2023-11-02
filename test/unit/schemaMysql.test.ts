import * as assert from 'assert'
import * as sinon from 'sinon'
import * as mysql from 'mysql'
import { MysqlDatabase } from '../../src/schemaMysql'
import { TableDefinition } from '../../src/schemaInterfaces'
import Options from '../../src/options'

const options = new Options({})

const MysqlDBReflection = MysqlDatabase as any

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

describe('MysqlDatabase', () => {
    let db: MysqlDatabase
    const sandbox = sinon.sandbox.create()
    before(() => {
        sandbox.stub(mysql, 'createConnection')
        sandbox.stub(MysqlDBReflection.prototype, 'queryAsync')
        db = new MysqlDatabase(process.env.DB_URL ?? 'mysql://user:password@localhost/test')
    })
    beforeEach(() => {
        sandbox.reset()
    })
    after(() => {
        sandbox.restore()
    })
    describe('query', () => {
        it('query calls query async', async () => {
            await db.query('SELECT * FROM test_table')
            assert.deepEqual(
                MysqlDBReflection.prototype.queryAsync.getCall(0).args,
                ['SELECT * FROM test_table']
            )
        })
    })
    describe('queryAsync', () => {
        before(() => {
            MysqlDBReflection.prototype.queryAsync.restore()
        })
        after(() => {
            sandbox.stub(MysqlDBReflection.prototype, 'queryAsync')
        })
        it('query has error', async () => {
            ;(mysql.createConnection as any).returns({
                query: function query(
                    queryString: string,
                    params: Array<any>,
                    cb: Function
                ) {
                    cb('ERROR')
                },
            })
            const testDb: any = new MysqlDatabase(
                'mysql://user:password@localhost/test'
            )
            try {
                await testDb.query('SELECT * FROM test_table')
            } catch (e) {
                assert.equal(e, 'ERROR')
            }
        })
        it('query returns with results', async () => {
            ;(mysql.createConnection as any).returns({
                query: function query(
                    queryString: string,
                    params: Array<any>,
                    cb: Function
                ) {
                    cb(null, [])
                },
            })
            const testDb: any = new MysqlDatabase(
                'mysql://user:password@localhost/test'
            )
            const results = await testDb.query('SELECT * FROM test_table')
            assert.deepEqual(results, [])
        })
    })
    describe('getEnumTypes', () => {
        it('writes correct query with schema name', async () => {
            MysqlDBReflection.prototype.queryAsync.returns(Promise.resolve([]))
            await db.getEnumTypes('testschema')
            assert.deepEqual(
                MysqlDBReflection.prototype.queryAsync.getCall(0).args,
                [
                    'SELECT column_name as `column_name`, column_type as `column_type`, data_type as `data_type` ' +
                        ', column_comment as `column_comment` ' +
                        'FROM information_schema.columns ' +
                        "WHERE data_type IN ('enum', 'set') and table_schema = ?",
                    ['testschema'],
                ]
            )
        })
        it('writes correct query without schema name', async () => {
            MysqlDBReflection.prototype.queryAsync.returns(Promise.resolve([]))
            await db.getEnumTypes()
            assert.deepEqual(
                MysqlDBReflection.prototype.queryAsync.getCall(0).args,
                [
                    'SELECT column_name as `column_name`, column_type as `column_type`, data_type as `data_type` ' +
                        ', column_comment as `column_comment` ' +
                        'FROM information_schema.columns ' +
                        "WHERE data_type IN ('enum', 'set') ",
                    [],
                ]
            )
        })
        it('handles response', async () => {
            MysqlDBReflection.prototype.queryAsync.returns(
                Promise.resolve([
                    {
                        column_name: 'column1',
                        column_type: "enum('enum1')",
                        data_type: 'enum',
                    },
                    {
                        column_name: 'column2',
                        column_type: "set('set1')",
                        data_type: 'set',
                    },
                ])
            )
            const enumTypes = await db.getEnumTypes('testschema')
            assert.deepEqual(enumTypes, {
                enum_column1: ['enum1'],
                set_column2: ['set1'],
            })
        })
        it('same column same value is accepted', async () => {
            MysqlDBReflection.prototype.queryAsync.returns(
                Promise.resolve([
                    {
                        column_name: 'column1',
                        column_type: "enum('enum1','enum2')",
                        data_type: 'enum',
                    },
                    {
                        column_name: 'column1',
                        column_type: "enum('enum1','enum2')",
                        data_type: 'enum',
                    },
                ])
            )
            const enumTypes = await db.getEnumTypes('testschema')
            assert.deepEqual(enumTypes, {
                enum_column1: ['enum1', 'enum2'],
            })
        })
        it('same column different value conflict', async () => {
            MysqlDBReflection.prototype.queryAsync.returns(
                Promise.resolve([
                    {
                        column_name: 'column1',
                        column_type: "enum('enum1')",
                        data_type: 'enum',
                    },
                    {
                        column_name: 'column1',
                        column_type: "enum('enum2')",
                        data_type: 'enum',
                    },
                ])
            )
            try {
                await db.getEnumTypes('testschema')
            } catch (e) {
                assert.equal(
                    e.message,
                    'Multiple enums with the same name and contradicting types were found: column1: ["enum1"] and ["enum2"]'
                )
            }
        })
    })
    describe('loadTableColumns', () => {
        it('writes correct query', async () => {
            MysqlDBReflection.prototype.queryAsync.returns(Promise.resolve([]))
            await db.loadTableColumns({tableName: 'testtable', schemaName: "testschema", columns:{}, comment: ""})
            assert.deepEqual(
                MysqlDBReflection.prototype.queryAsync.getCall(0).args,
                [
                    'SELECT column_name as `column_name`, data_type as `data_type`, is_nullable as `is_nullable`, column_default as `column_default` ' +
                        ', column_comment as `column_comment` ' +
                        'FROM information_schema.columns ' +
                        'WHERE table_name = ? and table_schema = ?',
                    ['testtable', 'testschema'],
                ]
            )
        })
        it('handles response', async () => {
            MysqlDBReflection.prototype.queryAsync.returns(
                Promise.resolve([
                    {
                        column_name: 'column1',
                        data_type: 'data1',
                        is_nullable: 'NO',
                        column_default: null,
                        comment : ""
                    },
                    {
                        column_name: 'column2',
                        data_type: 'enum',
                        is_nullable: 'YES',
                        column_default: null,
                        comment: ""
                    },
                    {
                        column_name: 'column3',
                        data_type: 'set',
                        is_nullable: 'YES',
                        column_default: null,
                        comment: ""
                    },
                ])
            )
            const schemaTables = await db.loadTableColumns({
                tableName: 'testtable',
                schemaName: 'testschema',
                columns: {},
                comment: ""
            })
            assert.deepEqual(schemaTables.columns, {
                column1: {
                    udtName: 'data1',
                    nullable: false,
                    defaultValue: null,
                    comment: "",
                },
                column2: {
                    udtName: 'enum_column2',
                    nullable: true,
                    defaultValue: null,
                    comment: "",
                },
                column3: {
                    udtName: 'set_column3',
                    nullable: true,
                    defaultValue: null,
                    comment: "",
                },
            })
        })
    })
    describe('getTableTypes', () => {
        const tableTypesSandbox = sinon.sandbox.create()
        before(() => {
            tableTypesSandbox.stub(MysqlDBReflection.prototype, 'getEnumTypes')
            tableTypesSandbox.stub(
                MysqlDBReflection.prototype,
                'loadTableColumns'
            )
            tableTypesSandbox.stub(
                MysqlDBReflection,
                'mapTableDefinitionToType'
            )
        })
        beforeEach(() => {
            tableTypesSandbox.reset()
        })
        after(() => {
            tableTypesSandbox.restore()
        })
        it('gets custom types from enums', async () => {
            MysqlDBReflection.prototype.getEnumTypes.returns(
                Promise.resolve({ enum1: [], enum2: [] })
            )
            MysqlDBReflection.prototype.loadTableColumns.returns(
                Promise.resolve({})
            )
            await db.getTableTypes({tableName: 'tableName', schemaName: 'tableSchema', columns: {}, comment: ""}, options)
            assert.deepEqual(
                MysqlDBReflection.mapTableDefinitionToType.getCall(0).args[1],
                ['enum1', 'enum2']
            )
        })
        it('gets table definitions', async () => {
            MysqlDBReflection.prototype.getEnumTypes.returns(
                Promise.resolve({})
            )
            MysqlDBReflection.prototype.loadTableColumns.returns(
                Promise.resolve({
                    table: {
                        udtName: 'name',
                        nullable: false,
                    },
                })
            )
            await db.getTableTypes({tableName: 'tableName', schemaName: 'tableSchema', columns: {}, comment: ""}, options)
            assert.deepEqual(
                MysqlDBReflection.prototype.loadTableColumns.getCall(0).args,
                [{
                    tableName: 'tableName', 
                    schemaName: 'tableSchema',
                    columns: {},
                    comment: "",
                }]
            )
            assert.deepEqual(
                MysqlDBReflection.mapTableDefinitionToType.getCall(0).args[0],
                {
                    table: {
                        udtName: 'name',
                        nullable: false,
                    },
                }
            )
        })
    })
    describe('getSchemaTables', () => {
        it('writes correct query', async () => {
            MysqlDBReflection.prototype.queryAsync.returns(Promise.resolve([]))
            await db.getSchemaTables('testschema')
            assert.deepEqual(
                MysqlDBReflection.prototype.queryAsync.getCall(0).args,
                [
                    'SELECT table_name as `table_name` ' +
                        ', table_schema as `table_schema`, table_comment as `table_comment` ' +
                        'FROM information_schema.tables ' +
                        'WHERE table_schema = ? ' +
                        'ORDER BY 1',
                    ['testschema'],
                ]
            )
        })
        it('handles table response', async () => {
            MysqlDBReflection.prototype.queryAsync.returns(
                Promise.resolve([
                    { table_name: 'table1' },
                    { table_name: 'table2' },
                ])
            )
            const schemaTables = await db.getSchemaTables('testschema')
            assert.deepEqual(schemaTables, [
                {
                    columns: {},
                    comment: "",
                    schemaName: undefined,
                    tableName: "table1",
                }, {
                    columns: {},
                    comment: "",
                    schemaName: undefined,
                    tableName: "table2",
                }
            ]);
        })
    })
    describe('mapTableDefinitionToType', () => {
        describe('maps to string', () => {
            it('char', () => {
                const td = tableDef({
                    column: {
                        udtName: 'char',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'string'
                )
            })
            it('varchar', () => {
                const td = tableDef({
                    column: {
                        udtName: 'varchar',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'string'
                )
            })
            it('text', () => {
                const td = tableDef({
                    column: {
                        udtName: 'text',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'string'
                )
            })
            it('tinytext', () => {
                const td = tableDef({
                    column: {
                        udtName: 'tinytext',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'string'
                )
            })
            it('mediumtext', () => {
                const td = tableDef({
                    column: {
                        udtName: 'mediumtext',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'string'
                )
            })
            it('longtext', () => {
                const td = tableDef({
                    column: {
                        udtName: 'longtext',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'string'
                )
            })
            it('time', () => {
                const td = tableDef({
                    column: {
                        udtName: 'time',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'string'
                )
            })
            it('geometry', () => {
                const td = tableDef({
                    column: {
                        udtName: 'geometry',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'string'
                )
            })
            it('set', () => {
                const td = tableDef({
                    column: {
                        udtName: 'set',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'string'
                )
            })
            it('enum', () => {
                const td = tableDef({
                    column: {
                        udtName: 'enum',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'string'
                )
            })
        })
        describe('maps to number', () => {
            it('integer', () => {
                const td = tableDef({
                    column: {
                        udtName: 'integer',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'number'
                )
            })
            it('int', () => {
                const td = tableDef({
                    column: {
                        udtName: 'int',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'number'
                )
            })
            it('smallint', () => {
                const td = tableDef({
                    column: {
                        udtName: 'smallint',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'number'
                )
            })
            it('mediumint', () => {
                const td = tableDef({
                    column: {
                        udtName: 'mediumint',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'number'
                )
            })
            it('bigint', () => {
                const td = tableDef({
                    column: {
                        udtName: 'bigint',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'number'
                )
            })
            it('double', () => {
                const td = tableDef({
                    column: {
                        udtName: 'double',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'number'
                )
            })
            it('decimal', () => {
                const td = tableDef({
                    column: {
                        udtName: 'decimal',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'number'
                )
            })
            it('numeric', () => {
                const td = tableDef({
                    column: {
                        udtName: 'numeric',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'number'
                )
            })
            it('float', () => {
                const td = tableDef({
                    column: {
                        udtName: 'float',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'number'
                )
            })
            it('year', () => {
                const td = tableDef({
                    column: {
                        udtName: 'year',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'number'
                )
            })
        })
        describe('maps to boolean', () => {
            it('tinyint', () => {
                const td = tableDef({
                    column: {
                        udtName: 'tinyint',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'boolean'
                )
            })
        })
        describe('maps json to any', () => {
            it('json', () => {
                const td = tableDef({
                    column: {
                        udtName: 'json',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'any'
                )
            })
        })
        describe('maps to Date', () => {
            it('date', () => {
                const td = tableDef({
                    column: {
                        udtName: 'date',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'Date'
                )
            })
            it('datetime', () => {
                const td = tableDef({
                    column: {
                        udtName: 'datetime',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'Date'
                )
            })
            it('timestamp', () => {
                const td = tableDef({
                    column: {
                        udtName: 'timestamp',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'Date'
                )
            })
        })
        describe('maps to Buffer', () => {
            it('tinyblob', () => {
                const td = tableDef({
                    column: {
                        udtName: 'tinyblob',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'Buffer'
                )
            })
            it('mediumblob', () => {
                const td = tableDef({
                    column: {
                        udtName: 'mediumblob',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'Buffer'
                )
            })
            it('longblob', () => {
                const td = tableDef({
                    column: {
                        udtName: 'longblob',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'Buffer'
                )
            })
            it('blob', () => {
                const td = tableDef({
                    column: {
                        udtName: 'blob',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'Buffer'
                )
            })
            it('binary', () => {
                const td = tableDef({
                    column: {
                        udtName: 'binary',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'Buffer'
                )
            })
            it('varbinary', () => {
                const td = tableDef({
                    column: {
                        udtName: 'varbinary',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'Buffer'
                )
            })
            it('bit', () => {
                const td = tableDef({
                    column: {
                        udtName: 'bit',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(td, [], options)
                        .column.tsType,
                    'Buffer'
                )
            })
        })
        describe('maps to custom', () => {
            it('CustomType', () => {
                const td = tableDef({
                    column: {
                        udtName: 'CustomType',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(
                        td,
                        ['CustomType'],
                        options
                    ).column.tsType,
                    'CustomType'
                )
            })
        })
        describe('maps to any', () => {
            it('UnknownType', () => {
                const td = tableDef({
                    column: {
                        udtName: 'UnknownType',
                        nullable: false,
                        defaultValue: null,
                    },
                });
                assert.equal(
                    MysqlDBReflection.mapTableDefinitionToType(
                        td,
                        ['CustomType'],
                        options
                    ).column.tsType,
                    'any'
                )
            })
        })
    })
})
