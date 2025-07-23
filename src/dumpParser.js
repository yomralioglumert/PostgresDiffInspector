import fs from 'fs/promises';
import path from 'path';

/**
 * PostgreSQL dump dosyalarını parse eden sınıf
 */
export class DumpParser {
  constructor() {
    this.tables = new Map();
    this.schema = 'public'; // varsayılan şema
  }

  /**
   * Dump dosyasını parse eder
   */
  async parseDumpFile(filePath) {
    try {
      const content = await fs.readFile(filePath, 'utf8');
      
      // Dosya boyutunu kontrol et
      const stats = await fs.stat(filePath);
      console.log(`📁 Dump dosyası okunuyor: ${path.basename(filePath)} (${(stats.size / 1024 / 1024).toFixed(2)} MB)`);
      
      this.parseContent(content);
      
      console.log(`✅ ${this.tables.size} tablo parse edildi`);
      return true;
    } catch (error) {
      console.error(`❌ Dump dosyası okunamadı: ${error.message}`);
      throw error;
    }
  }

  /**
   * Dump içeriğini parse eder
   */
  parseContent(content) {
    const lines = content.split('\n');
    let currentTable = null;
    let currentColumns = [];
    let isInsertSection = false;
    let insertBuffer = '';

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      
      // CREATE TABLE ifadelerini yakala
      if (line.startsWith('CREATE TABLE')) {
        const match = line.match(/CREATE TABLE (?:(\w+)\.)?(\w+)/);
        if (match) {
          const schema = match[1] || 'public';
          const tableName = match[2];
          
          if (schema === this.schema) {
            currentTable = tableName;
            currentColumns = [];
            this.tables.set(currentTable, {
              name: currentTable,
              columns: [],
              primaryKeys: [],
              foreignKeys: [],
              indexes: [],
              data: []
            });
          }
        }
      }
      
      // CREATE TABLE'dan sonraki satırları parse et
      else if (currentTable && !line.startsWith('--') && line.length > 0) {
        // Basit sütun tanımı kontrolü
        if (line.includes('integer') || line.includes('character varying') || 
            line.includes('varchar') || line.includes('text') || 
            line.includes('numeric') || line.includes('timestamp')) {
          
          const trimmedLine = line.trim().replace(/,$/, ''); // Sondaki virgülü kaldır
          
          // Sütun adını ve veri tipini parse et
          const match = trimmedLine.match(/^\s*(\w+)\s+(.+?)(?:\s+NOT\s+NULL|\s+DEFAULT|\s*$)/i);
          if (match) {
            const columnName = match[1];
            let dataType = match[2].trim();
            
            // Veri tipini temizle
            dataType = dataType.replace(/\s+NOT\s+NULL.*$/i, '').trim();
            dataType = dataType.replace(/\s+DEFAULT.*$/i, '').trim();
            
            const table = this.tables.get(currentTable);
            if (table) {
              table.columns.push({
                name: columnName,
                dataType: dataType,
                nullable: !line.includes('NOT NULL'),
                defaultValue: line.includes('DEFAULT') ? 'default' : null
              });
            }
          }
        }
        
        // Tablo tanımı bittiğinde
        if (line.includes(');')) {
          currentTable = null;
        }
      }
      
      // PRIMARY KEY ifadelerini yakala
      else if (line.includes('ADD CONSTRAINT') && line.includes('PRIMARY KEY')) {
        const match = line.match(/ALTER TABLE (?:\w+\.)?(\w+) ADD CONSTRAINT .+ PRIMARY KEY \(([^)]+)\)/);
        if (match && this.tables.has(match[1])) {
          const tableName = match[1];
          const pkColumns = match[2].split(',').map(col => col.trim().replace(/"/g, ''));
          this.tables.get(tableName).primaryKeys = pkColumns;
        }
      }
      
      // FOREIGN KEY ifadelerini yakala
      else if (line.includes('ADD CONSTRAINT') && line.includes('FOREIGN KEY')) {
        const match = line.match(/ALTER TABLE (?:\w+\.)?(\w+) ADD CONSTRAINT (\w+) FOREIGN KEY \(([^)]+)\) REFERENCES (?:\w+\.)?(\w+)\(([^)]+)\)/);
        if (match && this.tables.has(match[1])) {
          const tableName = match[1];
          this.tables.get(tableName).foreignKeys.push({
            name: match[2],
            columns: match[3].split(',').map(col => col.trim().replace(/"/g, '')),
            referencedTable: match[4],
            referencedColumns: match[5].split(',').map(col => col.trim().replace(/"/g, ''))
          });
        }
      }
      
      // INDEX ifadelerini yakala
      else if (line.startsWith('CREATE INDEX') || line.startsWith('CREATE UNIQUE INDEX')) {
        const match = line.match(/CREATE (?:UNIQUE )?INDEX (\w+) ON (?:\w+\.)?(\w+) \(([^)]+)\)/);
        if (match && this.tables.has(match[2])) {
          const tableName = match[2];
          this.tables.get(tableName).indexes.push({
            name: match[1],
            columns: match[3].split(',').map(col => col.trim().replace(/"/g, '')),
            unique: line.includes('UNIQUE')
          });
        }
      }
      
      // INSERT ifadelerini yakala
      else if (line.startsWith('INSERT INTO')) {
        if (line.endsWith(';')) {
          // Tek satırlık INSERT
          this.parseInsertStatement(line);
        } else {
          // Çok satırlık INSERT başlangıcı
          isInsertSection = true;
          insertBuffer = line;
        }
      } else if (isInsertSection) {
        insertBuffer += ' ' + line;
        
        if (line.endsWith(';')) {
          this.parseInsertStatement(insertBuffer);
          isInsertSection = false;
          insertBuffer = '';
        }
      }
    }
  }

  /**
   * CREATE TABLE tanımını parse eder
   */
  parseTableDefinition(tableName, content) {
    const table = this.tables.get(tableName);
    if (!table) return;

    // Sütun tanımlarını çıkar
    const match = content.match(/\(([^;]+)\)/);
    if (!match) return;

    const columnSection = match[1];
    const lines = columnSection.split(',');
    
    for (const line of lines) {
      const trimmed = line.trim();
      
      // Constraint ifadelerini atla
      if (trimmed.startsWith('CONSTRAINT') || trimmed.startsWith('PRIMARY KEY') || 
          trimmed.startsWith('FOREIGN KEY') || trimmed.startsWith('CHECK')) {
        continue;
      }
      
      // Sütun tanımını parse et
      const parts = trimmed.split(/\s+/);
      if (parts.length >= 2) {
        const columnName = parts[0].replace(/"/g, '');
        const dataType = parts[1];
        
        const column = {
          name: columnName,
          dataType: dataType,
          nullable: !trimmed.includes('NOT NULL'),
          defaultValue: null
        };
        
        // DEFAULT değerini yakala
        const defaultMatch = trimmed.match(/DEFAULT (.+?)(?:\s|$)/);
        if (defaultMatch) {
          column.defaultValue = defaultMatch[1];
        }
        
        table.columns.push(column);
      }
    }
  }

  /**
   * INSERT ifadesini parse eder
   */
  parseInsertStatement(statement) {
    const match = statement.match(/INSERT INTO (?:\w+\.)?(\w+) \(([^)]+)\) VALUES (.+);/);
    if (!match) return;

    const tableName = match[1];
    const columns = match[2].split(',').map(col => col.trim().replace(/"/g, ''));
    const valuesSection = match[3];

    if (!this.tables.has(tableName)) return;

    // Çoklu VALUES ifadelerini parse et
    const valueMatches = valuesSection.match(/\([^)]+\)/g);
    if (valueMatches) {
      for (const valueMatch of valueMatches) {
        const values = this.parseValues(valueMatch);
        if (values.length === columns.length) {
          const record = {};
          for (let i = 0; i < columns.length; i++) {
            record[columns[i]] = values[i];
          }
          this.tables.get(tableName).data.push(record);
        }
      }
    }
  }

  /**
   * VALUES kısmındaki değerleri parse eder
   */
  parseValues(valuesString) {
    // Parantezleri kaldır
    const content = valuesString.slice(1, -1);
    const values = [];
    let current = '';
    let inQuotes = false;
    let quoteChar = '';
    
    for (let i = 0; i < content.length; i++) {
      const char = content[i];
      
      if (!inQuotes && (char === "'" || char === '"')) {
        inQuotes = true;
        quoteChar = char;
        current += char;
      } else if (inQuotes && char === quoteChar) {
        // Escape edilmiş tırnak kontrolü
        if (content[i + 1] === quoteChar) {
          current += char + char;
          i++; // Bir sonraki karakteri atla
        } else {
          inQuotes = false;
          current += char;
        }
      } else if (!inQuotes && char === ',') {
        values.push(this.parseValue(current.trim()));
        current = '';
      } else {
        current += char;
      }
    }
    
    // Son değeri ekle
    if (current.trim()) {
      values.push(this.parseValue(current.trim()));
    }
    
    return values;
  }

  /**
   * Tek bir değeri parse eder
   */
  parseValue(value) {
    if (value === 'NULL') return null;
    if (value === 'TRUE') return true;
    if (value === 'FALSE') return false;
    
    // String değerler
    if ((value.startsWith("'") && value.endsWith("'")) || 
        (value.startsWith('"') && value.endsWith('"'))) {
      return value.slice(1, -1).replace(/''/g, "'").replace(/""/g, '"');
    }
    
    // Sayısal değerler
    if (/^-?\d+(\.\d+)?$/.test(value)) {
      return value.includes('.') ? parseFloat(value) : parseInt(value);
    }
    
    return value;
  }

  /**
   * Parse edilmiş tabloları döndürür
   */
  getTables() {
    return Array.from(this.tables.values());
  }

  /**
   * Belirli bir tabloyu döndürür
   */
  getTable(tableName) {
    return this.tables.get(tableName);
  }

  /**
   * Şema bilgilerini DatabaseComparator formatında döndürür
   */
  getSchemaInfo() {
    const tables = this.getTables();
    
    return {
      totalTables: tables.length,
      tableList: tables.map(table => ({
        name: table.name,
        columns: table.columns,
        primaryKeys: table.primaryKeys,
        foreignKeys: table.foreignKeys,
        indexes: table.indexes
      }))
    };
  }

  /**
   * Veri bilgilerini DataComparator formatında döndürür
   */
  getDataInfo() {
    const result = {};
    
    for (const [tableName, table] of this.tables) {
      result[tableName] = {
        records: table.data,
        totalRecords: table.data.length
      };
    }
    
    return result;
  }
} 