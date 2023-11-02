import Options from './options'

export interface ColumnDefinition {
    udtName: string
    nullable: boolean
    defaultValue: string | null
    tsType?: string
    tsCustomType?: boolean
    rawType?: string
    comment: string
}

export interface TableDefinition {
    schemaName: string
    tableName: string
    comment: string
    columns: { [columnName: string]: ColumnDefinition }
}

export interface Database {
    connectionString: string
    query(queryString: string): Promise<Object[]>
    getDefaultSchema(): string
    getEnumTypes(schema?: string): any
    loadTableColumns(
        table: TableDefinition
    ): Promise<TableDefinition>
    getTableTypes(
        table: TableDefinition,
        options: Options
    ): Promise<TableDefinition["columns"]>
    getSchemaTables(schemaName: string): Promise<TableDefinition[]>
}
