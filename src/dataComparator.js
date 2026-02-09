import pkg from 'pg';
const { Client } = pkg;
import chalk from 'chalk';
import ora from 'ora';
import { DumpParser } from './dumpParser.js';

export class DataComparator {
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
   * İki veritabanı arasında veri karşılaştırması yapar
   */
  async compareData(cloudUrl, edgeUrl, schema = 'public', options = {}) {
    let spinner;
    let cloudData, edgeData;
    let commonTables = [];

    // Parse table filter - supports both "table" and "schema.table" formats
    let tableFilter = null;
    if (options.tables && options.tables.length > 0) {
      tableFilter = options.tables.map(t => {
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
      // Cloud kaynağını belirle (DB veya dump)
      if (options.cloudDump) {
        spinner = ora('Cloud dump dosyası parse ediliyor...').start();
        const cloudParser = new DumpParser();
        await cloudParser.parseDumpFile(options.cloudDump);
        cloudData = cloudParser.getDataInfo();
        spinner.succeed('Cloud dump dosyası parse edildi');
      } else {
        spinner = ora('Cloud veritabanına bağlanılıyor...').start();
        this.cloudClient = await this.createClient(cloudUrl, options);
        spinner.succeed('Cloud veritabanına bağlantı başarılı');
      }

      // Edge kaynağını belirle (DB veya dump)
      if (options.edgeDump) {
        spinner = ora('Edge dump dosyası parse ediliyor...').start();
        const edgeParser = new DumpParser();
        await edgeParser.parseDumpFile(options.edgeDump);
        edgeData = edgeParser.getDataInfo();
        spinner.succeed('Edge dump dosyası parse edildi');
      } else {
        spinner = ora('Edge veritabanına bağlanılıyor...').start();
        this.edgeClient = await this.createClient(edgeUrl, options);
        spinner.succeed('Edge veritabanına bağlantı başarılı');
      }

      // Ortak tabloları bul
      spinner.start('Ortak tablolar bulunuyor...');
      if (options.cloudDump && options.edgeDump) {
        // Her iki taraf da dump ise, dump'lardan ortak tabloları bul
        const cloudTables = Object.keys(cloudData);
        const edgeTables = Object.keys(edgeData);
        commonTables = cloudTables.filter(table => edgeTables.includes(table));
      } else {
        // En az bir taraf DB ise, mevcut metodu kullan
        commonTables = await this.getCommonTables(schema, tableFilter);
      }

      // Apply table filter if provided (for dump sources)
      if (tableFilter && tableFilter.length > 0) {
        commonTables = commonTables.filter(table => tableFilter.includes(table));
      }

      spinner.succeed(`${commonTables.length} ortak tablo bulundu`);

      const results = {
        summary: {
          totalTables: commonTables.length,
          tablesWithDifferences: 0,
          totalMissingRecords: 0
        },
        tableResults: [],
        insertQueries: [],
        executionLog: []
      };

      // Her tablo için veri karşılaştırması yap
      for (const tableName of commonTables) {
        spinner.start(`${tableName} tablosu karşılaştırılıyor...`);

        let tableResult;
        if (options.cloudDump && options.edgeDump) {
          // Her iki taraf da dump ise, dump verilerini karşılaştır
          tableResult = await this.compareDumpTableData(tableName, cloudData, edgeData);
        } else {
          // En az bir taraf DB ise, karma karşılaştırma yap
          tableResult = await this.compareTableData(tableName, schema, options, cloudData, edgeData);
        }
        results.tableResults.push(tableResult);

        if (tableResult.hasDifferences) {
          results.summary.tablesWithDifferences++;
          results.summary.totalMissingRecords += tableResult.missingInEdge.length + tableResult.missingInCloud.length;

          // INSERT SQL'leri oluştur
          const insertQueries = this.generateInsertQueries(tableName, tableResult);
          results.insertQueries.push(...insertQueries);
        }

        spinner.succeed(`${tableName} tablosu tamamlandı`);
      }

      return results;

    } catch (error) {
      spinner.fail('Hata oluştu');
      throw error;
    } finally {
      // Bağlantıları kapat
      if (this.cloudClient) {
        await this.cloudClient.end();
      }
      if (this.edgeClient) {
        await this.edgeClient.end();
      }
    }
  }

  /**
   * Ortak tabloları bulur
   * @param {string} schema - Schema name
   * @param {string[]|null} tables - Optional array of specific tables to filter
   */
  async getCommonTables(schema, tables = null) {
    let sourceTablesQuery;
    let queryParams;

    if (tables && tables.length > 0) {
      const placeholders = tables.map((_, i) => `$${i + 2}`).join(', ');
      sourceTablesQuery = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $1 
        AND table_type = 'BASE TABLE'
        AND table_name IN (${placeholders})
        ORDER BY table_name
      `;
      queryParams = [schema, ...tables];
    } else {
      sourceTablesQuery = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $1 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
      `;
      queryParams = [schema];
    }

    const cloudResult = await this.cloudClient.query(sourceTablesQuery, queryParams);
    const edgeResult = await this.edgeClient.query(sourceTablesQuery, queryParams);

    const cloudTables = new Set(cloudResult.rows.map(row => row.table_name));
    const edgeTables = new Set(edgeResult.rows.map(row => row.table_name));

    return Array.from(cloudTables).filter(table => edgeTables.has(table));
  }

  /**
   * Tablo verilerini karşılaştırır
   */
  async compareTableData(tableName, schema, options = {}, cloudData = null, edgeData = null) {
    const result = {
      tableName,
      hasDifferences: false,
      missingInCloud: [],
      missingInEdge: [],
      totalCloudRecords: 0,
      totalEdgeRecords: 0
    };

    try {
      // Primary key'leri bul
      let pkColumns = [];

      if (this.cloudClient) {
        // DB bağlantısı varsa information_schema'dan al
        pkColumns = await this.getPrimaryKeyColumns(tableName, schema);
      } else {
        // Sadece dump varsa basit çıkarsama yap
        const sampleData = cloudData?.[tableName]?.records?.[0] || edgeData?.[tableName]?.records?.[0];
        if (sampleData?.id !== undefined) {
          pkColumns = ['id'];
        } else if (sampleData) {
          pkColumns = Object.keys(sampleData);
        }
      }

      if (pkColumns.length === 0) {
        console.log(chalk.yellow(`⚠️  ${tableName} tablosunda primary key bulunamadı, atlanıyor`));
        return result;
      }

      // Tüm sütunları al (sadece DB bağlantısı varsa)
      if (this.cloudClient) {
        const allColumns = await this.getTableColumns(tableName, schema);
      }

      let cloudRows, edgeRows;

      // Cloud verilerini al (DB veya dump)
      if (options.cloudDump && cloudData) {
        cloudRows = cloudData[tableName]?.records || [];
      } else {
        // Büyük tablolar için sayfalama kullan
        cloudRows = await this.getTableDataWithPagination(this.cloudClient, tableName, schema, pkColumns, options);
      }
      result.totalCloudRecords = cloudRows.length;

      // Edge verilerini al (DB veya dump)
      if (options.edgeDump && edgeData) {
        edgeRows = edgeData[tableName]?.records || [];
      } else {
        // Büyük tablolar için sayfalama kullan
        edgeRows = await this.getTableDataWithPagination(this.edgeClient, tableName, schema, pkColumns, options);
      }
      result.totalEdgeRecords = edgeRows.length;

      // Kayıtları karşılaştır
      const cloudRecords = new Map();
      const edgeRecords = new Map();

      // Cloud kayıtlarını indexle
      cloudRows.forEach(row => {
        const key = this.createRecordKey(row, pkColumns);
        cloudRecords.set(key, row);
      });

      // Edge kayıtlarını indexle
      edgeRows.forEach(row => {
        const key = this.createRecordKey(row, pkColumns);
        edgeRecords.set(key, row);
      });

      // Cloud'da olup Edge'de olmayan kayıtları bul
      for (const [key, record] of cloudRecords) {
        if (!edgeRecords.has(key)) {
          result.missingInEdge.push(record);
        }
      }

      // Edge'de olup Cloud'da olmayan kayıtları bul
      for (const [key, record] of edgeRecords) {
        if (!cloudRecords.has(key)) {
          result.missingInCloud.push(record);
        }
      }

      result.hasDifferences = result.missingInCloud.length > 0 || result.missingInEdge.length > 0;

    } catch (error) {
      console.log(chalk.red(`❌ ${tableName} tablosu karşılaştırılırken hata: ${error.message}`));

      // Timeout hatası özel durumu
      if (error.message.includes('timeout') || error.message.includes('Query read timeout')) {
        console.log(chalk.yellow(`💡 ${tableName} tablosu çok büyük olabilir. Sayfalama kullanarak tekrar deneniyor...`));
        try {
          // Daha küçük batch boyutu ile tekrar dene
          const retryOptions = { ...options, batchSize: Math.max(1000, (options.batchSize || 10000) / 2) };
          return await this.compareTableData(tableName, schema, retryOptions, cloudData, edgeData);
        } catch (retryError) {
          console.log(chalk.red(`❌ ${tableName} tablosu tekrar denemede de başarısız: ${retryError.message}`));
        }
      }
    }

    return result;
  }

  /**
   * Büyük tablolar için sayfalama ile veri alma
   */
  async getTableDataWithPagination(client, tableName, schema, pkColumns, options = {}) {
    const batchSize = options.batchSize || 10000; // Varsayılan 10k kayıt
    const maxRetries = options.maxRetries || 3;
    let allRows = [];
    let offset = 0;
    let hasMoreData = true;

    console.log(chalk.cyan(`📄 ${tableName} tablosu sayfalama ile alınıyor (batch: ${batchSize})...`));

    while (hasMoreData) {
      let retryCount = 0;
      let batchData = null;

      while (retryCount < maxRetries) {
        try {
          const query = `
            SELECT * FROM "${schema}"."${tableName}" 
            ORDER BY ${pkColumns.map(col => `"${col}"`).join(', ')}
            LIMIT ${batchSize} OFFSET ${offset}
          `;

          const result = await client.query(query);
          batchData = result.rows;
          break;

        } catch (error) {
          retryCount++;
          console.log(chalk.yellow(`⚠️  ${tableName} batch ${offset}-${offset + batchSize} hata (deneme ${retryCount}/${maxRetries}): ${error.message}`));

          if (retryCount >= maxRetries) {
            throw new Error(`${tableName} tablosu ${maxRetries} denemede başarısız: ${error.message}`);
          }

          // Exponential backoff
          await new Promise(resolve => setTimeout(resolve, Math.pow(2, retryCount) * 1000));
        }
      }

      if (batchData && batchData.length > 0) {
        allRows.push(...batchData);
        offset += batchSize;
        hasMoreData = batchData.length === batchSize;

        // İlerleme göster
        if (offset % (batchSize * 5) === 0) {
          console.log(chalk.gray(`  📊 ${tableName}: ${allRows.length} kayıt alındı...`));
        }
      } else {
        hasMoreData = false;
      }
    }

    console.log(chalk.green(`✅ ${tableName}: Toplam ${allRows.length} kayıt alındı`));
    return allRows;
  }

  /**
   * Dump verilerini karşılaştırır
   */
  async compareDumpTableData(tableName, cloudData, edgeData) {
    const result = {
      tableName,
      hasDifferences: false,
      missingInCloud: [],
      missingInEdge: [],
      totalCloudRecords: 0,
      totalEdgeRecords: 0
    };

    try {
      // Dump verilerinden kayıtları al
      const cloudRecords = cloudData[tableName]?.records || [];
      const edgeRecords = edgeData[tableName]?.records || [];

      result.totalCloudRecords = cloudRecords.length;
      result.totalEdgeRecords = edgeRecords.length;

      // Primary key sütunlarını belirle (dump'tan çıkarsamaya çalış)
      // Basit yaklaşım: 'id' sütunu varsa onu kullan, yoksa tüm sütunları kullan
      let pkColumns = [];
      if (cloudRecords.length > 0) {
        const sampleRecord = cloudRecords[0];
        if (sampleRecord.id !== undefined) {
          pkColumns = ['id'];
        } else {
          // Tüm sütunları kullan (basit yaklaşım)
          pkColumns = Object.keys(sampleRecord);
        }
      } else if (edgeRecords.length > 0) {
        const sampleRecord = edgeRecords[0];
        if (sampleRecord.id !== undefined) {
          pkColumns = ['id'];
        } else {
          pkColumns = Object.keys(sampleRecord);
        }
      }

      if (pkColumns.length === 0) {
        console.log(chalk.yellow(`⚠️  ${tableName} tablosunda sütun bulunamadı, atlanıyor`));
        return result;
      }

      // Kayıtları Map'e dönüştür
      const cloudRecordMap = new Map();
      const edgeRecordMap = new Map();

      // Cloud kayıtlarını indexle
      cloudRecords.forEach(record => {
        const key = this.createRecordKey(record, pkColumns);
        cloudRecordMap.set(key, record);
      });

      // Edge kayıtlarını indexle
      edgeRecords.forEach(record => {
        const key = this.createRecordKey(record, pkColumns);
        edgeRecordMap.set(key, record);
      });

      // Cloud'da olup Edge'de olmayan kayıtları bul
      for (const [key, record] of cloudRecordMap) {
        if (!edgeRecordMap.has(key)) {
          result.missingInEdge.push(record);
        }
      }

      // Edge'de olup Cloud'da olmayan kayıtları bul
      for (const [key, record] of edgeRecordMap) {
        if (!cloudRecordMap.has(key)) {
          result.missingInCloud.push(record);
        }
      }

      result.hasDifferences = result.missingInCloud.length > 0 || result.missingInEdge.length > 0;

    } catch (error) {
      console.log(chalk.red(`❌ ${tableName} dump karşılaştırılırken hata: ${error.message}`));
    }

    return result;
  }

  /**
   * Primary key sütunlarını alır
   */
  async getPrimaryKeyColumns(tableName, schema) {
    const query = `
      SELECT kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
      WHERE tc.constraint_type = 'PRIMARY KEY' 
      AND tc.table_schema = $1 
      AND tc.table_name = $2
      ORDER BY kcu.ordinal_position
    `;

    const result = await this.cloudClient.query(query, [schema, tableName]);
    return result.rows.map(row => row.column_name);
  }

  /**
   * Tablo sütunlarını alır
   */
  async getTableColumns(tableName, schema) {
    const query = `
      SELECT column_name, data_type
      FROM information_schema.columns 
      WHERE table_schema = $1 
      AND table_name = $2
      ORDER BY ordinal_position
    `;

    const result = await this.cloudClient.query(query, [schema, tableName]);
    return result.rows;
  }

  /**
   * Kayıt için benzersiz anahtar oluşturur
   */
  createRecordKey(record, pkColumns) {
    return pkColumns.map(col => record[col]).join('|');
  }



  /**
   * INSERT SQL'leri oluşturur
   */
  generateInsertQueries(tableName, tableResult) {
    const queries = [];

    // Cloud'da olup Edge'de olmayan kayıtlar için INSERT (Edge'e eklenecek)
    if (tableResult.missingInEdge.length > 0) {
      const columns = Object.keys(tableResult.missingInEdge[0]);
      const valuesList = tableResult.missingInEdge.map(record => {
        const values = columns.map(col => this.formatValue(record[col]));
        return `(${values.join(', ')})`;
      });

      const insertQuery = `INSERT INTO "${tableName}" (${columns.map(col => `"${col}"`).join(', ')}) VALUES ${valuesList.join(', ')};`;
      queries.push({
        type: 'INSERT_TO_EDGE',
        tableName,
        recordCount: tableResult.missingInEdge.length,
        query: insertQuery,
        description: 'Cloud\'dan Edge\'e eksik kayıtlar'
      });
    }

    // Edge'de olup Cloud'da olmayan kayıtlar için INSERT (Cloud'a eklenecek)
    if (tableResult.missingInCloud.length > 0) {
      const columns = Object.keys(tableResult.missingInCloud[0]);
      const valuesList = tableResult.missingInCloud.map(record => {
        const values = columns.map(col => this.formatValue(record[col]));
        return `(${values.join(', ')})`;
      });

      const insertQuery = `INSERT INTO "${tableName}" (${columns.map(col => `"${col}"`).join(', ')}) VALUES ${valuesList.join(', ')};`;
      queries.push({
        type: 'INSERT_TO_CLOUD',
        tableName,
        recordCount: tableResult.missingInCloud.length,
        query: insertQuery,
        description: 'Edge\'den Cloud\'a eksik kayıtlar'
      });
    }

    return queries;
  }

  /**
   * SQL değerlerini formatlar
   */
  formatValue(value) {
    if (value === null || value === undefined) {
      return 'NULL';
    }

    if (typeof value === 'string') {
      return `'${value.replace(/'/g, "''")}'`;
    }

    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE';
    }

    if (value instanceof Date) {
      return `'${value.toISOString()}'`;
    }

    // PostgreSQL özel veri tipleri için formatla
    if (typeof value === 'object' && value !== null) {
      // Boş obje ise NULL döndür
      if (Object.keys(value).length === 0) {
        return 'NULL';
      }

      // PostgreSQL interval objesi ise (örn: {hours: 5, minutes: 30})
      if (value.years !== undefined || value.months !== undefined || value.days !== undefined ||
        value.hours !== undefined || value.minutes !== undefined || value.seconds !== undefined) {

        const parts = [];
        if (value.years) parts.push(`${value.years} years`);
        if (value.months) parts.push(`${value.months} months`);
        if (value.days) parts.push(`${value.days} days`);
        if (value.hours) parts.push(`${value.hours} hours`);
        if (value.minutes) parts.push(`${value.minutes} minutes`);
        if (value.seconds) parts.push(`${value.seconds} seconds`);

        if (parts.length === 0) return 'NULL';
        return `'${parts.join(' ')}'::interval`;
      }

      // Diğer objeler için JSON formatla
      return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
    }

    // Array ise JSON olarak formatla
    if (Array.isArray(value)) {
      return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
    }

    return String(value);
  }

  /**
   * Organize edilmiş INSERT SQL'lerini uygun veritabanlarına uygular
   */
  async executeAllInsertQueries(insertQueries, cloudUrl, edgeUrl, options = {}) {
    const results = {
      success: 0,
      failed: 0,
      errors: [],
      cloudResults: { success: 0, failed: 0, errors: [] },
      edgeResults: { success: 0, failed: 0, errors: [] }
    };

    // Cloud'a eklenecek kayıtlar (Edge'den gelen)
    const cloudQueries = insertQueries.filter(q => q.type === 'INSERT_TO_CLOUD');
    if (cloudQueries.length > 0) {
      console.log(chalk.cyan('🌐 Cloud veritabanına kayıtlar ekleniyor...'));
      const cloudResult = await this.executeInsertQueries(cloudQueries, cloudUrl, options);
      results.cloudResults = cloudResult;
      results.success += cloudResult.success;
      results.failed += cloudResult.failed;
      results.errors.push(...cloudResult.errors);
    }

    // Edge'e eklenecek kayıtlar (Cloud'dan gelen)
    const edgeQueries = insertQueries.filter(q => q.type === 'INSERT_TO_EDGE');
    if (edgeQueries.length > 0) {
      console.log(chalk.cyan('🏢 Edge veritabanına kayıtlar ekleniyor...'));
      const edgeResult = await this.executeInsertQueries(edgeQueries, edgeUrl, options);
      results.edgeResults = edgeResult;
      results.success += edgeResult.success;
      results.failed += edgeResult.failed;
      results.errors.push(...edgeResult.errors);
    }

    return results;
  }

  /**
   * INSERT SQL'lerini hedef veritabanına uygular
   */
  async executeInsertQueries(insertQueries, targetUrl, options = {}) {
    const results = {
      success: 0,
      failed: 0,
      errors: []
    };

    try {
      const client = await this.createClient(targetUrl, options);

      if (options.dryRun) {
        console.log(chalk.yellow('🔍 DRY RUN MODE - SQL\'ler çalıştırılmayacak'));
        return results;
      }

      for (const queryInfo of insertQueries) {
        try {
          await client.query(queryInfo.query);
          results.success++;
          console.log(chalk.green(`✅ ${queryInfo.tableName} tablosuna ${queryInfo.recordCount} kayıt eklendi`));
        } catch (error) {
          results.failed++;
          results.errors.push({
            table: queryInfo.tableName,
            error: error.message
          });
          console.log(chalk.red(`❌ ${queryInfo.tableName} tablosuna kayıt eklenirken hata: ${error.message}`));
        }
      }

    } catch (error) {
      throw new Error(`Veritabanı bağlantı hatası: ${error.message}`);
    } finally {
      await client.end();
    }

    return results;
  }

  /**
   * SQL dosyası oluşturur
   */
  generateSqlFile(insertQueries, filename) {
    const sqlContent = [
      '-- Otomatik oluşturulan INSERT SQL\'leri',
      `-- Oluşturulma tarihi: ${new Date().toISOString()}`,
      '',
      ...insertQueries.map(q => [
        `-- ${q.type} - ${q.tableName} tablosuna ${q.recordCount} kayıt`,
        `-- ${q.description}`,
        q.query,
        ''
      ].join('\n'))
    ].join('\n');

    return sqlContent;
  }

  /**
   * Organize edilmiş klasör yapısında dosyalar oluşturur
   */
  async generateOrganizedOutput(insertQueries, baseOutputPath = process.env.OUTPUT_DIR || 'output') {
    const fs = await import('fs/promises');

    // Ana klasörleri oluştur
    await fs.mkdir(`${baseOutputPath}/cloud-to-edge`, { recursive: true });
    await fs.mkdir(`${baseOutputPath}/edge-to-cloud`, { recursive: true });

    // Cloud'dan Edge'e SQL'leri
    const cloudToEdgeQueries = insertQueries.filter(q => q.type === 'INSERT_TO_EDGE');
    if (cloudToEdgeQueries.length > 0) {
      const sqlContent = this.generateSqlFile(cloudToEdgeQueries, 'cloud-to-edge.sql');
      await fs.writeFile(`${baseOutputPath}/cloud-to-edge/missing-records.sql`, sqlContent, 'utf8');

      // JSON raporu da oluştur
      const jsonReport = {
        timestamp: new Date().toISOString(),
        direction: 'Cloud → Edge',
        totalQueries: cloudToEdgeQueries.length,
        totalRecords: cloudToEdgeQueries.reduce((sum, q) => sum + q.recordCount, 0),
        tables: cloudToEdgeQueries.map(q => ({
          tableName: q.tableName,
          recordCount: q.recordCount,
          description: q.description
        }))
      };
      await fs.writeFile(`${baseOutputPath}/cloud-to-edge/report.json`, JSON.stringify(jsonReport, null, 2), 'utf8');
    }

    // Edge'den Cloud'a SQL'leri
    const edgeToCloudQueries = insertQueries.filter(q => q.type === 'INSERT_TO_CLOUD');
    if (edgeToCloudQueries.length > 0) {
      const sqlContent = this.generateSqlFile(edgeToCloudQueries, 'edge-to-cloud.sql');
      await fs.writeFile(`${baseOutputPath}/edge-to-cloud/missing-records.sql`, sqlContent, 'utf8');

      // JSON raporu da oluştur
      const jsonReport = {
        timestamp: new Date().toISOString(),
        direction: 'Edge → Cloud',
        totalQueries: edgeToCloudQueries.length,
        totalRecords: edgeToCloudQueries.reduce((sum, q) => sum + q.recordCount, 0),
        tables: edgeToCloudQueries.map(q => ({
          tableName: q.tableName,
          recordCount: q.recordCount,
          description: q.description
        }))
      };
      await fs.writeFile(`${baseOutputPath}/edge-to-cloud/report.json`, JSON.stringify(jsonReport, null, 2), 'utf8');
    }

    // Genel özet raporu
    const summaryReport = {
      timestamp: new Date().toISOString(),
      summary: {
        totalQueries: insertQueries.length,
        totalRecords: insertQueries.reduce((sum, q) => sum + q.recordCount, 0),
        cloudToEdge: {
          queries: cloudToEdgeQueries.length,
          records: cloudToEdgeQueries.reduce((sum, q) => sum + q.recordCount, 0)
        },
        edgeToCloud: {
          queries: edgeToCloudQueries.length,
          records: edgeToCloudQueries.reduce((sum, q) => sum + q.recordCount, 0)
        }
      },
      details: insertQueries
    };

    await fs.writeFile(`${baseOutputPath}/summary-report.json`, JSON.stringify(summaryReport, null, 2), 'utf8');

    return {
      cloudToEdgeQueries: cloudToEdgeQueries.length,
      edgeToCloudQueries: edgeToCloudQueries.length,
      totalRecords: summaryReport.summary.totalRecords
    };
  }
} 