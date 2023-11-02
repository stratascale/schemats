import * as assert from 'assert'
import * as sinon from 'sinon'
import * as Index from '../../src/index'
import * as Typescript from '../../src/typescript'
import { Database } from '../../src/schema'
import Options, { OptionValues } from '../../src/options'
import { TableDefinition } from '../../src/schemaInterfaces'

const options: OptionValues = {}

describe('index', () => {
    const typedTableSandbox = sinon.sandbox.create()
    const db = {
        getDefaultSchema: typedTableSandbox.stub(),
        getTableTypes: typedTableSandbox.stub(),
        query: typedTableSandbox.stub(),
        getEnumTypes: typedTableSandbox.stub(),
        loadTableColumns: typedTableSandbox.stub(),
        getSchemaTables: typedTableSandbox.stub(),
        connectionString: 'sql://'
    } as Database
    const tsReflection = Typescript as any
    const dbReflection = db as any
    before(() => {
        typedTableSandbox.stub(Typescript, 'generateEnumType')
        typedTableSandbox.stub(Typescript, 'generateTableTypes')
        typedTableSandbox.stub(Typescript, 'generateTableInterface')
    })
    beforeEach(() => {
        typedTableSandbox.reset()
    })
    after(() => {
        typedTableSandbox.restore()
    })
    describe('typescriptOfTable', () => {
        it('calls functions with correct params', async () => {
            dbReflection.getTableTypes.returns(Promise.resolve('tableTypes'))
            await Index.typescriptOfTable(
                db,
                {
                    tableName: 'tableName',
                    schemaName: 'schemaName',
                    columns: {},
                    comment: ""
                },
                new Options({ tableNamespaces: true })
            )
            const expected = {
                tableName: 'tableName',
                schemaName: 'schemaName',
                columns: 'tableTypes',
                comment: ""
            };
            const expected2 = 'tableTypes';
            assert.deepEqual(dbReflection.getTableTypes.getCall(0).args, [
                expected,
                new Options({ tableNamespaces: true })
            ])
            assert.deepEqual(tsReflection.generateTableTypes.getCall(0).args, [
                expected,
                new Options({ tableNamespaces: true })
            ])
            assert.deepEqual(
                tsReflection.generateTableInterface.getCall(0).args,
                [
                    expected,
                    new Options({ tableNamespaces: true })
                ]
            )
        })
        it('merges string results', async () => {
            dbReflection.getTableTypes.returns(Promise.resolve('tableTypes'))
            tsReflection.generateTableTypes.returns('generatedTableTypes\n')
            tsReflection.generateTableInterface.returns(
                'generatedTableInterfaces\n'
            )
            const typescriptString = await Index.typescriptOfTable(
                db,
                {
                    tableName: 'tableName',
                    schemaName: 'schemaName',
                    columns: {},
                    comment: ""
                },
                new Options({ tableNamespaces: true })
            )
            assert.equal(
                typescriptString,
                'generatedTableTypes\ngeneratedTableInterfaces\n'
            )
        })
    })
    describe('typescriptOfSchema', () => {
        it('has schema', async () => {
            dbReflection.getSchemaTables.returns(Promise.resolve([{
                tableName: 'tablename',
                schema: '',
                columns: {},
                comment: '',
            }]));
            dbReflection.getEnumTypes.returns(Promise.resolve('enumTypes'))
            tsReflection.generateTableTypes.returns('generatedTableTypes\n')
            tsReflection.generateEnumType.returns('generatedEnumTypes\n')
            const tsOfSchema = await Index.typescriptOfSchema(
                db,
                [],
                'schemaName',
                { tableNamespaces: true }
            )

            assert.deepEqual(
                dbReflection.getSchemaTables.getCall(0).args[0],
                'schemaName'
            )
            assert.deepEqual(
                dbReflection.getEnumTypes.getCall(0).args[0],
                'schemaName'
            )
            assert.deepEqual(
                tsReflection.generateEnumType.getCall(0).args[0],
                'enumTypes'
            )
            /*
             TODO
            assert.deepEqual(
                tsReflection.generateTableTypes.getCall(0).args[0],
                'tablename'
            )
            */
        })
        it('has tables provided', async () => {
            dbReflection.getSchemaTables.returns(Promise.resolve([{
                tableName: 'tablename',
                schemaName: '',
                columns: {},
                comment: "",
            }]));
            dbReflection.getEnumTypes.returns(Promise.resolve('enumTypes'))
            tsReflection.generateTableTypes.returns('generatedTableTypes\n')
            tsReflection.generateEnumType.returns('generatedEnumTypes\n')
            const tsOfSchema = await Index.typescriptOfSchema(
                db,
                ['differentTablename'],
                null,
                { tableNamespaces: true }
            )

            assert(!dbReflection.getSchemaTables.called)
            assert.deepEqual(
                tsReflection.generateEnumType.getCall(0).args[0],
                'enumTypes'
            )
            assert.deepEqual(
                tsReflection.generateTableTypes.getCall(0).args[0],
                {
                    columns: undefined,
                    comment: '',
                    schemaName: '',
                    tableName: 'differentTablename',
                }
            )
        })
    })
})
