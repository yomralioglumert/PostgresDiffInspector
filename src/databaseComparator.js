import pkg from 'pg';
const { Client } = pkg;
import chalk from 'chalk';
import ora from 'ora';
import fs from 'fs/promises';
import { DumpParser } from './dumpParser.js';

export class DatabaseComparator {
  constructor() {
    this.sourceClient = null;
    this.targetClient = null;
  }

  /**
   * Create database client with SSL fallback
   */
  async createClient(url, options = {}) {
    // Configurable timeout values with larger defaults for big tables
    const timeoutMs = options.timeout || 120000; // 2 minutes default
    const queryTimeoutMs = options.queryTimeout || 300000; // 5 minutes for queries

    const baseConfig = {
      connectionString: url,
      connectionTimeoutMillis: timeoutMs,
      query_timeout: queryTimeoutMs,
      statement_timeout: queryTimeoutMs,
      idle_in_transaction_session_timeout: timeoutMs
    };

    // First try with SSL
    let config = { ...baseConfig };
    if (url.includes('sslmode=require')) {
      config.ssl = { rejectUnauthorized: false };
    } else if (url.includes('sslmode=disable')) {
      config.ssl = false;
    } else {
      config.ssl = { rejectUnauthorized: false };
    }

    let client = new Client(config);

    try {
      await client.connect();
      return client;
    } catch (error) {
      console.log(chalk.yellow(`SSL connection failed: ${error.message}`));
      if (client) {
        await client.end();
      }

      // If SSL failed and not explicitly required, try without SSL
      if (!url.includes('sslmode=require') && !url.includes('sslmode=disable')) {
        console.log(chalk.yellow('Trying connection without SSL...'));
        config = { ...baseConfig, ssl: false };
        client = new Client(config);

        try {
          await client.connect();
          console.log(chalk.green('Non-SSL connection successful'));
          return client;
        } catch (error2) {
          console.error(chalk.red(`Non-SSL connection also failed: ${error2.message}`));
          if (client) {
            await client.end();
          }
          throw error2;
        }
      } else {
        throw error;
      }
    }
  }

  /**
   * Test database connection
   */
  async testConnection(url, options = {}) {
    // Configurable timeout values with larger defaults for big tables
    const timeoutMs = options.timeout || 120000; // 2 minutes default
    const queryTimeoutMs = options.queryTimeout || 300000; // 5 minutes for queries

    // First try with SSL
    let config = {
      connectionString: url,
      connectionTimeoutMillis: timeoutMs,
      query_timeout: queryTimeoutMs,
      statement_timeout: queryTimeoutMs,
      idle_in_transaction_session_timeout: timeoutMs
    };

    // Add SSL configuration
    if (url.includes('sslmode=require')) {
      config.ssl = { rejectUnauthorized: false };
    } else if (url.includes('sslmode=disable')) {
      // Explicitly disable SSL
      config.ssl = false;
    } else {
      // Default: try SSL first, fallback to non-SSL
      config.ssl = { rejectUnauthorized: false };
    }

    let client = new Client(config);

    try {
      await client.connect();
      await client.end();
      return true;
    } catch (error) {
      console.error('SSL connection failed:', error.message);
      if (client) {
        await client.end();
      }

      // If SSL failed and not explicitly required, try without SSL
      if (!url.includes('sslmode=require') && !url.includes('sslmode=disable')) {
        console.log('Trying connection without SSL...');
        config.ssl = false;
        client = new Client(config);

        try {
          await client.connect();
          await client.end();
          console.log('Non-SSL connection successful');
          return true;
        } catch (error2) {
          console.error('Non-SSL connection also failed:', error2.message);
          if (client) {
            await client.end();
          }
        }
      }

      return false;
    }
  }

  /**
   * Compare schemas between two databases
   */
  async compareDatabases(sourceUrl, targetUrl, schema = 'public', verbose = false, dumpOptions = {}) {
    let spinner;
    let sourceSchema, targetSchema;

    // Parse table filter - supports both "table" and "schema.table" formats
    let tableFilter = null;
    if (dumpOptions.tables && dumpOptions.tables.length > 0) {
      tableFilter = dumpOptions.tables.map(t => {
        if (t.includes('.')) {
          const [tableSchema, tableName] = t.split('.');
          // If schema matches, use just the table name
          if (tableSchema === schema) {
            return tableName;
          }
          // If different schema, we'll skip this table for this schema
          return null;
        }
        return t;
      }).filter(t => t !== null);
    }

    try {
      // Determine cloud source (DB or dump)
      if (dumpOptions.cloudDump) {
        spinner = ora('Parsing cloud dump file...').start();
        const cloudParser = new DumpParser();
        await cloudParser.parseDumpFile(dumpOptions.cloudDump);
        sourceSchema = cloudParser.getSchemaInfo();
        spinner.succeed('Cloud dump file parsed');
      } else {
        spinner = ora('Connecting to cloud database...').start();
        this.sourceClient = await this.createClient(sourceUrl, dumpOptions);
        spinner.succeed('Cloud database connection successful');

        spinner.start('Getting cloud schema information...');
        sourceSchema = await this.getSchemaInfo(this.sourceClient, schema, tableFilter);
        spinner.succeed('Cloud schema information retrieved');
      }

      // Determine edge source (DB or dump)
      if (dumpOptions.edgeDump) {
        spinner = ora('Parsing edge dump file...').start();
        const edgeParser = new DumpParser();
        await edgeParser.parseDumpFile(dumpOptions.edgeDump);
        targetSchema = edgeParser.getSchemaInfo();
        spinner.succeed('Edge dump file parsed');
      } else {
        spinner = ora('Connecting to edge database...').start();
        this.targetClient = await this.createClient(targetUrl, dumpOptions);
        spinner.succeed('Edge database connection successful');

        spinner.start('Getting edge schema information...');
        targetSchema = await this.getSchemaInfo(this.targetClient, schema, tableFilter);
        spinner.succeed('Edge schema information retrieved');
      }

      // Perform comparison
      spinner.start('Performing comparison...');
      const comparison = this.compareSchemas(sourceSchema, targetSchema, verbose);
      spinner.succeed('Comparison completed');

      return comparison;

    } catch (error) {
      spinner.fail('Error occurred');
      throw error;
    } finally {
      // Close connections
      if (this.sourceClient) {
        await this.sourceClient.end();
      }
      if (this.targetClient) {
        await this.targetClient.end();
      }
    }
  }

  /**
   * Get database schema information
   * @param {object} client - Database client
   * @param {string} schema - Schema name
   * @param {string[]|null} tables - Optional array of specific tables to include
   */
  async getSchemaInfo(client, schema, tables = null) {
    const schemaInfo = {
      totalTables: 0,
      tableList: [],
      tables: {}
    };

    // Get table list
    let tablesQuery;
    let queryParams;

    if (tables && tables.length > 0) {
      // Filter by specific tables
      const placeholders = tables.map((_, i) => `$${i + 2}`).join(', ');
      tablesQuery = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $1 
        AND table_type = 'BASE TABLE'
        AND table_name IN (${placeholders})
        ORDER BY table_name
      `;
      queryParams = [schema, ...tables];
    } else {
      tablesQuery = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $1 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
      `;
      queryParams = [schema];
    }

    const tablesResult = await client.query(tablesQuery, queryParams);
    schemaInfo.totalTables = tablesResult.rows.length;

    // Get detailed information for each table
    for (const row of tablesResult.rows) {
      const tableName = row.table_name;
      const tableInfo = await this.getTableInfo(client, tableName, schema);

      schemaInfo.tableList.push(tableInfo);
      schemaInfo.tables[tableName] = tableInfo;
    }

    return schemaInfo;
  }

  /**
   * Get detailed table information
   */
  async getTableInfo(client, tableName, schema) {
    const tableInfo = {
      name: tableName,
      columns: [],
      primaryKeys: [],
      foreignKeys: [],
      indexes: []
    };

    // Get column information
    const columnsQuery = `
      SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length,
        numeric_precision,
        numeric_scale
      FROM information_schema.columns 
      WHERE table_schema = $1 
      AND table_name = $2
      ORDER BY ordinal_position
    `;

    const columnsResult = await client.query(columnsQuery, [schema, tableName]);
    tableInfo.columns = columnsResult.rows.map(col => ({
      name: col.column_name,
      dataType: col.data_type,
      nullable: col.is_nullable === 'YES',
      defaultValue: col.column_default,
      maxLength: col.character_maximum_length,
      precision: col.numeric_precision,
      scale: col.numeric_scale
    }));

    // Get primary keys
    const primaryKeysQuery = `
      SELECT kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
      WHERE tc.constraint_type = 'PRIMARY KEY' 
      AND tc.table_schema = $1 
      AND tc.table_name = $2
      ORDER BY kcu.ordinal_position
    `;

    const primaryKeysResult = await client.query(primaryKeysQuery, [schema, tableName]);
    tableInfo.primaryKeys = primaryKeysResult.rows.map(row => row.column_name);

    // Get foreign keys
    const foreignKeysQuery = `
      SELECT 
        tc.constraint_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
      FROM information_schema.table_constraints AS tc 
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
      JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
      WHERE tc.constraint_type = 'FOREIGN KEY' 
      AND tc.table_schema = $1
      AND tc.table_name = $2
    `;

    const foreignKeysResult = await client.query(foreignKeysQuery, [schema, tableName]);
    const fkMap = {};

    foreignKeysResult.rows.forEach(row => {
      if (!fkMap[row.constraint_name]) {
        fkMap[row.constraint_name] = {
          name: row.constraint_name,
          columns: [],
          referencedTable: row.foreign_table_name,
          referencedColumns: []
        };
      }
      fkMap[row.constraint_name].columns.push(row.column_name);
      fkMap[row.constraint_name].referencedColumns.push(row.foreign_column_name);
    });

    tableInfo.foreignKeys = Object.values(fkMap);

    // Get indexes
    const indexesQuery = `
      SELECT 
        indexname,
        indexdef
      FROM pg_indexes 
      WHERE schemaname = $1 
      AND tablename = $2
    `;

    const indexesResult = await client.query(indexesQuery, [schema, tableName]);
    tableInfo.indexes = indexesResult.rows.map(row => ({
      name: row.indexname,
      definition: row.indexdef,
      unique: row.indexdef.includes('UNIQUE')
    }));

    return tableInfo;
  }

  /**
   * Compare two schemas
   */
  compareSchemas(sourceSchema, targetSchema, verbose = false) {
    // Extract table names
    const sourceTableNames = sourceSchema.tableList.map(table => typeof table === 'string' ? table : table.name);
    const targetTableNames = targetSchema.tableList.map(table => typeof table === 'string' ? table : table.name);

    const sourceTables = new Set(sourceTableNames);
    const targetTables = new Set(targetTableNames);

    // Common tables
    const commonTables = sourceTableNames.filter(table => targetTables.has(table));

    // Tables only in source
    const onlyInSource = sourceTableNames.filter(table => !targetTables.has(table));

    // Tables only in target
    const onlyInTarget = targetTableNames.filter(table => !sourceTables.has(table));

    // Find differences in common tables
    const tableDifferences = [];
    for (const tableName of commonTables) {
      // Find table information
      const sourceTable = sourceSchema.tables ?
        sourceSchema.tables[tableName] :
        sourceSchema.tableList.find(t => t.name === tableName);
      const targetTable = targetSchema.tables ?
        targetSchema.tables[tableName] :
        targetSchema.tableList.find(t => t.name === tableName);

      if (sourceTable && targetTable) {
        const differences = this.compareTable(sourceTable, targetTable, tableName);

        if (differences.hasDifferences) {
          tableDifferences.push(differences);
        }
      }
    }

    // Generate CREATE SQL for missing tables
    const createTableQueries = [];

    // CREATE SQLs for tables in Cloud but not in Edge
    for (const tableName of onlyInSource) {
      const tableInfo = sourceSchema.tableList.find(t => t.name === tableName);
      if (tableInfo) {
        const createSql = this.generateCreateTableSQL(tableInfo, 'CREATE_IN_EDGE');
        createTableQueries.push({
          type: 'CREATE_IN_EDGE',
          tableName: tableName,
          sql: createSql,
          description: `Create ${tableName} table from Cloud in Edge`
        });
      }
    }

    // CREATE SQLs for tables in Edge but not in Cloud
    for (const tableName of onlyInTarget) {
      const tableInfo = targetSchema.tableList.find(t => t.name === tableName);
      if (tableInfo) {
        const createSql = this.generateCreateTableSQL(tableInfo, 'CREATE_IN_CLOUD');
        createTableQueries.push({
          type: 'CREATE_IN_CLOUD',
          tableName: tableName,
          sql: createSql,
          description: `Create ${tableName} table from Edge in Cloud`
        });
      }
    }

    return {
      sourceStats: {
        totalTables: sourceSchema.totalTables
      },
      targetStats: {
        totalTables: targetSchema.totalTables
      },
      commonTables,
      onlyInSource,
      onlyInTarget,
      tableDifferences,
      createTableQueries,
      summary: {
        totalMissingTables: onlyInSource.length + onlyInTarget.length,
        missingInEdge: onlyInSource.length,
        missingInCloud: onlyInTarget.length,
        totalCreateQueries: createTableQueries.length
      },
      detailedComparison: verbose ? {
        sourceSchema,
        targetSchema
      } : null
    };
  }

  /**
   * Compare two tables
   */
  compareTable(sourceTable, targetTable, tableName) {
    const differences = {
      tableName,
      hasDifferences: false,
      columnDifferences: [],
      constraintDifferences: [],
      indexDifferences: []
    };

    // Column comparison
    const sourceColumns = sourceTable.columns || [];
    const targetColumns = targetTable.columns || [];

    const sourceColMap = new Map(sourceColumns.map(col => [col.name, col]));
    const targetColMap = new Map(targetColumns.map(col => [col.name, col]));

    // Check columns in source but not in target
    for (const [colName, colInfo] of sourceColMap) {
      if (!targetColMap.has(colName)) {
        differences.columnDifferences.push({
          columnName: colName,
          difference: `Column exists in source but not in target`
        });
        differences.hasDifferences = true;
      }
    }

    // Check columns in target but not in source
    for (const [colName, colInfo] of targetColMap) {
      if (!sourceColMap.has(colName)) {
        differences.columnDifferences.push({
          columnName: colName,
          difference: `Column exists in target but not in source`
        });
        differences.hasDifferences = true;
      }
    }

    // Check common columns for differences
    for (const [colName, sourceCol] of sourceColMap) {
      const targetCol = targetColMap.get(colName);
      if (targetCol) {
        if (sourceCol.dataType !== targetCol.dataType) {
          differences.columnDifferences.push({
            columnName: colName,
            difference: `Data type difference: ${sourceCol.dataType} vs ${targetCol.dataType}`
          });
          differences.hasDifferences = true;
        }

        if (sourceCol.nullable !== targetCol.nullable) {
          differences.columnDifferences.push({
            columnName: colName,
            difference: `Nullable difference: ${sourceCol.nullable} vs ${targetCol.nullable}`
          });
          differences.hasDifferences = true;
        }
      }
    }

    // Primary key comparison
    const sourcePKs = (sourceTable.primaryKeys || []).sort();
    const targetPKs = (targetTable.primaryKeys || []).sort();

    if (JSON.stringify(sourcePKs) !== JSON.stringify(targetPKs)) {
      differences.constraintDifferences.push({
        constraintName: 'PRIMARY KEY',
        difference: `Primary key difference: [${sourcePKs.join(', ')}] vs [${targetPKs.join(', ')}]`
      });
      differences.hasDifferences = true;
    }

    // Foreign key comparison
    const sourceFKs = (sourceTable.foreignKeys || []).map(fk => fk.name).sort();
    const targetFKs = (targetTable.foreignKeys || []).map(fk => fk.name).sort();

    if (JSON.stringify(sourceFKs) !== JSON.stringify(targetFKs)) {
      differences.constraintDifferences.push({
        constraintName: 'FOREIGN KEYS',
        difference: `Foreign key difference: [${sourceFKs.join(', ')}] vs [${targetFKs.join(', ')}]`
      });
      differences.hasDifferences = true;
    }

    // Index comparison
    const sourceIndexes = (sourceTable.indexes || []).map(idx => idx.name).sort();
    const targetIndexes = (targetTable.indexes || []).map(idx => idx.name).sort();

    if (JSON.stringify(sourceIndexes) !== JSON.stringify(targetIndexes)) {
      differences.indexDifferences.push({
        indexName: 'ALL INDEXES',
        difference: `Index difference: [${sourceIndexes.join(', ')}] vs [${targetIndexes.join(', ')}]`
      });
      differences.hasDifferences = true;
    }

    return differences;
  }

  /**
   * Save comparison report to file
   */
  async saveReport(result, filename) {
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        sourceStats: result.sourceStats,
        targetStats: result.targetStats,
        commonTables: result.commonTables,
        onlyInSource: result.onlyInSource,
        onlyInTarget: result.onlyInTarget,
        tableDifferences: result.tableDifferences
      }
    };

    await fs.writeFile(filename, JSON.stringify(report, null, 2), 'utf8');
  }

  /**
   * Generate CREATE TABLE SQL for a table
   */
  generateCreateTableSQL(tableInfo, queryType) {
    const tableName = tableInfo.name;
    let sql = `-- ${queryType} - Create ${tableName} table\n`;
    sql += `CREATE TABLE "${tableName}" (\n`;

    // Add columns
    const columnDefinitions = [];
    for (const column of tableInfo.columns) {
      let columnDef = `    "${column.name}" ${column.dataType}`;

      // NOT NULL check
      if (!column.nullable) {
        columnDef += ' NOT NULL';
      }

      // DEFAULT value
      if (column.defaultValue && column.defaultValue !== 'NULL') {
        columnDef += ` DEFAULT ${column.defaultValue}`;
      }

      columnDefinitions.push(columnDef);
    }

    sql += columnDefinitions.join(',\n');
    sql += '\n);\n\n';

    // Add primary key constraint
    if (tableInfo.primaryKeys && tableInfo.primaryKeys.length > 0) {
      const pkColumns = tableInfo.primaryKeys.map(pk => `"${pk}"`).join(', ');
      sql += `-- Primary key constraint\n`;
      sql += `ALTER TABLE "${tableName}" ADD CONSTRAINT "${tableName}_pkey" PRIMARY KEY (${pkColumns});\n\n`;
    }

    // Add foreign key constraints
    if (tableInfo.foreignKeys && tableInfo.foreignKeys.length > 0) {
      sql += `-- Foreign key constraints\n`;
      for (const fk of tableInfo.foreignKeys) {
        const fkColumns = fk.columns.map(col => `"${col}"`).join(', ');
        const refColumns = fk.referencedColumns.map(col => `"${col}"`).join(', ');
        sql += `ALTER TABLE "${tableName}" ADD CONSTRAINT "${fk.name}" `;
        sql += `FOREIGN KEY (${fkColumns}) REFERENCES "${fk.referencedTable}"(${refColumns});\n`;
      }
      sql += '\n';
    }

    // Add indexes
    if (tableInfo.indexes && tableInfo.indexes.length > 0) {
      sql += `-- Indexes\n`;
      for (const index of tableInfo.indexes) {
        const indexColumns = index.columns.map(col => `"${col}"`).join(', ');
        const uniqueKeyword = index.unique ? 'UNIQUE ' : '';
        sql += `CREATE ${uniqueKeyword}INDEX "${index.name}" ON "${tableName}" (${indexColumns});\n`;
      }
      sql += '\n';
    }

    return sql;
  }

  /**
   * Generate organized CREATE TABLE output
   */
  async generateOrganizedSchemaOutput(createTableQueries, baseOutputPath = process.env.OUTPUT_DIR || 'output') {
    // Create main output directory
    await fs.mkdir(baseOutputPath, { recursive: true });

    // Create subdirectories
    const cloudToEdgePath = `${baseOutputPath}/cloud-to-edge`;
    const edgeToCloudPath = `${baseOutputPath}/edge-to-cloud`;

    await fs.mkdir(cloudToEdgePath, { recursive: true });
    await fs.mkdir(edgeToCloudPath, { recursive: true });

    // Cloud → Edge SQLs (tables missing in Edge)
    const cloudToEdgeQueries = createTableQueries.filter(q => q.type === 'CREATE_IN_EDGE');
    const edgeToCloudQueries = createTableQueries.filter(q => q.type === 'CREATE_IN_CLOUD');

    // Cloud → Edge SQL file
    if (cloudToEdgeQueries.length > 0) {
      let sql = `-- Auto-generated CREATE TABLE SQLs\n`;
      sql += `-- Tables missing in Edge database\n`;
      sql += `-- Generated at: ${new Date().toISOString()}\n\n`;

      for (const query of cloudToEdgeQueries) {
        sql += query.sql;
      }

      await fs.writeFile(`${cloudToEdgePath}/missing-tables.sql`, sql, 'utf8');

      // Report file
      const report = {
        timestamp: new Date().toISOString(),
        type: 'CREATE_IN_EDGE',
        description: 'Tables missing in Edge database',
        tables: cloudToEdgeQueries.map(q => ({
          tableName: q.tableName,
          description: q.description
        }))
      };

      await fs.writeFile(`${cloudToEdgePath}/report.json`, JSON.stringify(report, null, 2), 'utf8');
    }

    // Edge → Cloud SQL file
    if (edgeToCloudQueries.length > 0) {
      let sql = `-- Auto-generated CREATE TABLE SQLs\n`;
      sql += `-- Tables missing in Cloud database\n`;
      sql += `-- Generated at: ${new Date().toISOString()}\n\n`;

      for (const query of edgeToCloudQueries) {
        sql += query.sql;
      }

      await fs.writeFile(`${edgeToCloudPath}/missing-tables.sql`, sql, 'utf8');

      // Report file
      const report = {
        timestamp: new Date().toISOString(),
        type: 'CREATE_IN_CLOUD',
        description: 'Tables missing in Cloud database',
        tables: edgeToCloudQueries.map(q => ({
          tableName: q.tableName,
          description: q.description
        }))
      };

      await fs.writeFile(`${edgeToCloudPath}/report.json`, JSON.stringify(report, null, 2), 'utf8');
    }

    // Summary report
    const summaryReport = {
      timestamp: new Date().toISOString(),
      summary: {
        totalMissingTables: createTableQueries.length,
        missingInEdge: cloudToEdgeQueries.length,
        missingInCloud: edgeToCloudQueries.length
      },
      details: {
        cloudToEdge: cloudToEdgeQueries.map(q => q.tableName),
        edgeToCloud: edgeToCloudQueries.map(q => q.tableName)
      }
    };

    await fs.writeFile(`${baseOutputPath}/schema-summary-report.json`, JSON.stringify(summaryReport, null, 2), 'utf8');

    return {
      cloudToEdgeQueries: cloudToEdgeQueries.length,
      edgeToCloudQueries: edgeToCloudQueries.length,
      totalQueries: createTableQueries.length
    };
  }

  /**
   * Execute CREATE TABLE SQLs
   */
  async executeCreateTableQueries(createTableQueries, cloudUrl, edgeUrl, options = {}) {
    const results = {
      cloud: { success: 0, failed: 0, errors: [] },
      edge: { success: 0, failed: 0, errors: [] }
    };

    // Tables to be created in Edge (from Cloud)
    const edgeQueries = createTableQueries.filter(q => q.type === 'CREATE_IN_EDGE');
    if (edgeQueries.length > 0 && edgeUrl) {
      console.log(chalk.blue('🏢 Creating tables in Edge database...'));

      for (const query of edgeQueries) {
        try {
          if (options.dryRun) {
            console.log(chalk.yellow(`[DRY RUN] ${query.tableName} table would be created`));
            results.edge.success++;
          } else {
            // Connect to Edge DB and create table
            const edgeClient = await this.createClient(edgeUrl, options);
            await edgeClient.query(query.sql);
            await edgeClient.end();

            console.log(chalk.green(`✅ ${query.tableName} table created in Edge`));
            results.edge.success++;
          }
        } catch (error) {
          console.log(chalk.red(`❌ Error creating ${query.tableName} table: ${error.message}`));
          results.edge.failed++;
          results.edge.errors.push(`${query.tableName}: ${error.message}`);
        }
      }
    }

    // Tables to be created in Cloud (from Edge)
    const cloudQueries = createTableQueries.filter(q => q.type === 'CREATE_IN_CLOUD');
    if (cloudQueries.length > 0 && cloudUrl) {
      console.log(chalk.blue('☁️ Creating tables in Cloud database...'));

      for (const query of cloudQueries) {
        try {
          if (options.dryRun) {
            console.log(chalk.yellow(`[DRY RUN] ${query.tableName} table would be created`));
            results.cloud.success++;
          } else {
            // Connect to Cloud DB and create table
            const cloudClient = await this.createClient(cloudUrl, options);
            await cloudClient.query(query.sql);
            await cloudClient.end();

            console.log(chalk.green(`✅ ${query.tableName} table created in Cloud`));
            results.cloud.success++;
          }
        } catch (error) {
          console.log(chalk.red(`❌ Error creating ${query.tableName} table: ${error.message}`));
          results.cloud.failed++;
          results.cloud.errors.push(`${query.tableName}: ${error.message}`);
        }
      }
    }

    return results;
  }
} 