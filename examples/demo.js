#!/usr/bin/env node

/**
 * DB Karşılaştırıcı CLI - Kullanım Örnekleri
 * 
 * Bu dosya, CLI aracının nasıl kullanılacağını gösteren örnekler içerir.
 */

import { DatabaseComparator } from '../src/databaseComparator.js';
import chalk from 'chalk';

console.log(chalk.blue.bold('🚀 Postgres Diff Inspector (PDI) - Demo'));
console.log(chalk.gray('='.repeat(50)));

// Örnek 1: Şema karşılaştırması
console.log(chalk.yellow.bold('\n📋 Örnek 1: Şema Karşılaştırması'));
console.log(chalk.gray('Komut:'));
console.log(chalk.cyan('pdi compare \\'));
console.log(chalk.cyan('  -s "postgresql://user:pass@localhost:5432/localdb" \\'));
console.log(chalk.cyan('  -t "postgresql://user:pass@cloud.com:5432/clouddb"'));
console.log(chalk.gray('Açıklama: İki veritabanı arasında şema yapısını karşılaştırır'));

// Örnek 2: Veri karşılaştırması
console.log(chalk.yellow.bold('\n📋 Örnek 2: Veri Karşılaştırması'));
console.log(chalk.gray('Komut:'));
console.log(chalk.cyan('pdi compare-data \\'));
console.log(chalk.cyan('  -s "postgresql://user:pass@localhost:5432/localdb" \\'));
console.log(chalk.cyan('  -t "postgresql://user:pass@cloud.com:5432/clouddb"'));
console.log(chalk.gray('Açıklama: İki veritabanı arasındaki eksik kayıtları bulur'));

// Örnek 3: SQL dosyası oluşturma
console.log(chalk.yellow.bold('\n📋 Örnek 3: SQL Dosyası Oluşturma'));
console.log(chalk.gray('Komut:'));
console.log(chalk.cyan('pdi compare-data \\'));
console.log(chalk.cyan('  -s "postgresql://user:pass@localhost:5432/localdb" \\'));
console.log(chalk.cyan('  -t "postgresql://user:pass@cloud.com:5432/clouddb" \\'));
console.log(chalk.cyan('  -o "sync.sql"'));
console.log(chalk.gray('Açıklama: Eksik kayıtlar için INSERT SQL\'leri oluşturur'));

// Örnek 4: Otomatik ekleme
console.log(chalk.yellow.bold('\n📋 Örnek 4: Otomatik Ekleme'));
console.log(chalk.gray('Komut:'));
console.log(chalk.cyan('pdi compare-data \\'));
console.log(chalk.cyan('  -s "postgresql://user:pass@localhost:5432/localdb" \\'));
console.log(chalk.cyan('  -t "postgresql://user:pass@cloud.com:5432/clouddb" \\'));
console.log(chalk.cyan('  --execute'));
console.log(chalk.gray('Açıklama: Eksik kayıtları otomatik olarak ekler (DİKKATLİ KULLANIN!)'));

// Örnek 5: Dry run (test modu)
console.log(chalk.yellow.bold('\n📋 Örnek 5: Dry Run (Test Modu)'));
console.log(chalk.gray('Komut:'));
console.log(chalk.cyan('pdi compare-data \\'));
console.log(chalk.cyan('  -s "postgresql://user:pass@localhost:5432/localdb" \\'));
console.log(chalk.cyan('  -t "postgresql://user:pass@cloud.com:5432/clouddb" \\'));
console.log(chalk.cyan('  --execute --dry-run'));
console.log(chalk.gray('Açıklama: SQL\'leri çalıştırmadan önizleme yapar'));

// Örnek 6: Bağlantı testi
console.log(chalk.yellow.bold('\n📋 Örnek 6: Bağlantı Testi'));
console.log(chalk.gray('Komut:'));
console.log(chalk.cyan('pdi test-connection \\'));
console.log(chalk.cyan('  -u "postgresql://user:pass@localhost:5432/testdb"'));
console.log(chalk.gray('Açıklama: Veritabanı bağlantısını test eder'));

// Örnek 7: Environment variables kullanımı
console.log(chalk.yellow.bold('\n📋 Örnek 7: Environment Variables'));
console.log(chalk.gray('env.example dosyasını .env olarak kopyalayın ve düzenleyin:'));
console.log(chalk.cyan('cp env.example .env'));
console.log(chalk.gray('Sonra .env dosyasındaki değişkenleri kullanabilirsiniz'));

console.log(chalk.gray('\n' + '='.repeat(50)));
console.log(chalk.green.bold('✨ Demo tamamlandı!'));
console.log(chalk.gray('\nDaha fazla bilgi için: pdi --help')); 