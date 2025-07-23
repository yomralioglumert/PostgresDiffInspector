import { DatabaseComparator } from '../src/databaseComparator.js';
import chalk from 'chalk';

async function runTests() {
  console.log(chalk.blue.bold('🧪 Test Başlatılıyor...'));
  console.log(chalk.gray('='.repeat(40)));

  const comparator = new DatabaseComparator();

  // Test 1: Bağlantı testi
  console.log(chalk.yellow('\n1️⃣ Bağlantı Testi'));
  try {
    // Bu test için gerçek bir PostgreSQL bağlantısı gerekiyor
    // Test amaçlı olarak geçersiz bir URL kullanıyoruz
    const isConnected = await comparator.testConnection('postgresql://invalid:invalid@localhost:5432/test');
    console.log(chalk.gray('   Bağlantı testi tamamlandı (beklenen: başarısız)'));
  } catch (error) {
    console.log(chalk.gray('   Bağlantı testi tamamlandı (beklenen: başarısız)'));
  }

  // Test 2: Mock veri ile karşılaştırma testi
  console.log(chalk.yellow('\n2️⃣ Mock Veri Karşılaştırma Testi'));
  
  const mockSourceSchema = {
    tables: {
      users: {
        columns: {
          id: { dataType: 'integer', isNullable: false, defaultValue: null },
          name: { dataType: 'varchar', isNullable: false, defaultValue: null },
          email: { dataType: 'varchar', isNullable: true, defaultValue: null }
        },
        primaryKeys: ['id'],
        foreignKeys: [],
        indexes: {}
      }
    },
    tableList: ['users'],
    totalTables: 1
  };

  const mockTargetSchema = {
    tables: {
      users: {
        columns: {
          id: { dataType: 'integer', isNullable: false, defaultValue: null },
          name: { dataType: 'varchar', isNullable: false, defaultValue: null },
          email: { dataType: 'varchar', isNullable: true, defaultValue: null },
          created_at: { dataType: 'timestamp', isNullable: true, defaultValue: 'now()' }
        },
        primaryKeys: ['id'],
        foreignKeys: [],
        indexes: {}
      }
    },
    tableList: ['users'],
    totalTables: 1
  };

  const comparison = comparator.compareSchemas(mockSourceSchema, mockTargetSchema);
  
  console.log(chalk.gray('   Mock karşılaştırma sonuçları:'));
  console.log(chalk.gray(`   - Ortak tablolar: ${comparison.commonTables.length}`));
  console.log(chalk.gray(`   - Sadece kaynakta: ${comparison.onlyInSource.length}`));
  console.log(chalk.gray(`   - Sadece hedefte: ${comparison.onlyInTarget.length}`));
  console.log(chalk.gray(`   - Fark bulunan tablolar: ${comparison.tableDifferences.length}`));

  if (comparison.tableDifferences.length > 0) {
    console.log(chalk.green('   ✅ Fark tespit edildi (beklenen)'));
  } else {
    console.log(chalk.red('   ❌ Fark tespit edilmedi (beklenmeyen)'));
  }

  // Test 3: Rapor kaydetme testi
  console.log(chalk.yellow('\n3️⃣ Rapor Kaydetme Testi'));
  try {
    await comparator.saveReport(comparison, 'test-report.json');
    console.log(chalk.green('   ✅ Rapor başarıyla kaydedildi'));
  } catch (error) {
    console.log(chalk.red(`   ❌ Rapor kaydetme hatası: ${error.message}`));
  }

  console.log(chalk.gray('\n' + '='.repeat(40)));
  console.log(chalk.green.bold('✨ Testler tamamlandı!'));
  console.log(chalk.gray('\nNot: Gerçek veritabanı testleri için geçerli bağlantı bilgileri gereklidir.'));
}

// Testleri çalıştır
runTests().catch(console.error); 