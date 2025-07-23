#!/usr/bin/env node

import { Command } from 'commander';
import chalk from 'chalk';
import dotenv from 'dotenv';
import fs from 'fs/promises';
import { DatabaseComparator } from './databaseComparator.js';
import { DataComparator } from './dataComparator.js';
import { DumpParser } from './dumpParser.js';

// Load environment variables
dotenv.config();

const program = new Command();

program
  .name('pdi')
  .description('Postgres Diff Inspector - CLI tool for schema and data comparison between PostgreSQL databases')
  .version('1.0.0');

program
  .command('schema')
  .description('Compare and synchronize database schemas between two PostgreSQL databases')
  .option('-c, --cloud <url>', 'Cloud database connection URL', process.env.CLOUD_DB_URL)
  .option('-e, --edge <url>', 'Edge database connection URL', process.env.EDGE_DB_URL)
  .option('--cloud-dump <file>', 'Cloud database dump file')
  .option('--edge-dump <file>', 'Edge database dump file')
  .option('-s, --schema <name>', 'Schema name to compare', process.env.DEFAULT_SCHEMA || 'public')
  .option('-o, --output <file>', 'Output file (optional)')
  .option('-v, --verbose', 'Verbose output')
  .option('--execute', 'Automatically create missing tables')
  .option('--dry-run', 'Preview SQL execution without running')
  .action(async (options) => {
    try {
      // Parameter validation
      const hasCloudUrl = options.cloud;
      const hasEdgeUrl = options.edge;
      const hasCloudDump = options.cloudDump;
      const hasEdgeDump = options.edgeDump;
      
      if (!hasCloudUrl && !hasCloudDump) {
        console.error(chalk.red('❌ Cloud source required: --cloud URL, --cloud-dump file, or CLOUD_DB_URL environment variable'));
        process.exit(1);
      }
      
      if (!hasEdgeUrl && !hasEdgeDump) {
        console.error(chalk.red('❌ Edge source required: --edge URL, --edge-dump file, or EDGE_DB_URL environment variable'));
        process.exit(1);
      }
      
      console.log(chalk.blue.bold('🔍 Postgres Diff Inspector (PDI) - Schema Comparison'));
      console.log(chalk.gray('='.repeat(50)));
      
      const comparator = new DatabaseComparator();
      
      console.log(chalk.yellow('📊 Starting comparison...'));
      if (hasCloudUrl) console.log(chalk.gray(`Cloud DB: ${options.cloud}`));
      if (hasCloudDump) console.log(chalk.gray(`Cloud Dump: ${options.cloudDump}`));
      if (hasEdgeUrl) console.log(chalk.gray(`Edge DB: ${options.edge}`));
      if (hasEdgeDump) console.log(chalk.gray(`Edge Dump: ${options.edgeDump}`));
      console.log(chalk.gray(`Schema: ${options.schema}`));
      if (options.execute) {
        console.log(chalk.red('⚠️  AUTOMATIC TABLE CREATION MODE ACTIVE!'));
      }
      if (options.dryRun) {
        console.log(chalk.yellow('🔍 DRY RUN MODE ACTIVE!'));
      }
      console.log('');
      
      const result = await comparator.compareDatabases(
        options.cloud,
        options.edge,
        options.schema,
        options.verbose,
        {
          cloudDump: options.cloudDump,
          edgeDump: options.edgeDump
        }
      );
      
      // Display results
      displayResults(result, options.verbose);
      
      // Handle missing tables if any
      if (result.createTableQueries && result.createTableQueries.length > 0) {
        console.log(chalk.blue.bold('\n🔧 Missing Table Operations'));
        console.log(chalk.gray('='.repeat(50)));
        
        // Generate organized output
        const organizedOutput = await comparator.generateOrganizedSchemaOutput(result.createTableQueries);
        console.log(chalk.green(`📁 Organized schema output created:`));
        console.log(chalk.gray(`   • Cloud → Edge: ${organizedOutput.cloudToEdgeQueries} tables`));
        console.log(chalk.gray(`   • Edge → Cloud: ${organizedOutput.edgeToCloudQueries} tables`));
        console.log(chalk.gray(`   • Total missing tables: ${organizedOutput.totalQueries}`));
        
        // Execute mode - create tables
        if (options.execute) {
          console.log(chalk.blue('\n🚀 Creating missing tables...'));
          
          const executionResults = await comparator.executeCreateTableQueries(
            result.createTableQueries,
            options.cloud,
            options.edge,
            { dryRun: options.dryRun }
          );
          
          // Display results
          console.log(chalk.blue.bold('\n📋 Table Creation Results:'));
          console.log(chalk.gray(`  • Total Successful: ${executionResults.cloud.success + executionResults.edge.success}`));
          console.log(chalk.gray(`  • Total Failed: ${executionResults.cloud.failed + executionResults.edge.failed}`));
          
          if (executionResults.cloud.success > 0 || executionResults.cloud.failed > 0) {
            console.log(chalk.blue('\n☁️ Cloud Database:'));
            console.log(chalk.gray(`    • Successful: ${executionResults.cloud.success}`));
            console.log(chalk.gray(`    • Failed: ${executionResults.cloud.failed}`));
          }
          
          if (executionResults.edge.success > 0 || executionResults.edge.failed > 0) {
            console.log(chalk.blue('\n🏢 Edge Database:'));
            console.log(chalk.gray(`    • Successful: ${executionResults.edge.success}`));
            console.log(chalk.gray(`    • Failed: ${executionResults.edge.failed}`));
          }
          
          // Display errors
          const allErrors = [...executionResults.cloud.errors, ...executionResults.edge.errors];
          if (allErrors.length > 0) {
            console.log(chalk.red.bold('\n❌ Errors:'));
            allErrors.forEach(error => {
              console.log(chalk.red(`  • ${error}`));
            });
          }
        }
      }
      
      // Save report if output file specified
      if (options.output) {
        await comparator.saveReport(result, options.output);
        console.log(chalk.green(`📄 Report saved: ${options.output}`));
      }
      
    } catch (error) {
      console.error(chalk.red.bold('❌ Error:'), error.message);
      if (options.verbose) {
        console.error(chalk.red(error.stack));
      }
      process.exit(1);
    }
  });

program
  .command('health')
  .description('Test database connection health')
  .requiredOption('-u, --url <url>', 'Database connection URL')
  .action(async (options) => {
    try {
      console.log(chalk.blue.bold('🔌 Connection Test'));
      console.log(chalk.gray('='.repeat(30)));
      
      const comparator = new DatabaseComparator();
      const isConnected = await comparator.testConnection(options.url);
      
      if (isConnected) {
        console.log(chalk.green('✅ Connection successful!'));
      } else {
        console.log(chalk.red('❌ Connection failed!'));
        process.exit(1);
      }
      
    } catch (error) {
      console.error(chalk.red.bold('❌ Error:'), error.message);
      process.exit(1);
    }
  });

program
  .command('records')
  .description('Compare and synchronize database records between two PostgreSQL databases')
  .option('-c, --cloud <url>', 'Cloud database connection URL', process.env.CLOUD_DB_URL)
  .option('-e, --edge <url>', 'Edge database connection URL', process.env.EDGE_DB_URL)
  .option('--cloud-dump <file>', 'Cloud database dump file')
  .option('--edge-dump <file>', 'Edge database dump file')
  .option('-s, --schema <name>', 'Schema name to compare', process.env.DEFAULT_SCHEMA || 'public')
  .option('-o, --output <file>', 'SQL output file (optional)')
  .option('-v, --verbose', 'Verbose output')
  .option('--execute', 'Automatically insert missing records')
  .option('--dry-run', 'Preview SQL execution without running')
  .action(async (options) => {
    try {
      // Parameter validation
      const hasCloudUrl = options.cloud;
      const hasEdgeUrl = options.edge;
      const hasCloudDump = options.cloudDump;
      const hasEdgeDump = options.edgeDump;
      
      if (!hasCloudUrl && !hasCloudDump) {
        console.error(chalk.red('❌ Cloud source required: --cloud URL, --cloud-dump file, or CLOUD_DB_URL environment variable'));
        process.exit(1);
      }
      
      if (!hasEdgeUrl && !hasEdgeDump) {
        console.error(chalk.red('❌ Edge source required: --edge URL, --edge-dump file, or EDGE_DB_URL environment variable'));
        process.exit(1);
      }
      
      console.log(chalk.blue.bold('🔍 Postgres Diff Inspector (PDI) - Data Comparison'));
      console.log(chalk.gray('='.repeat(50)));
      
      const dataComparator = new DataComparator();
      
      console.log(chalk.yellow('📊 Starting data comparison...'));
      if (hasCloudUrl) console.log(chalk.gray(`Cloud DB: ${options.cloud}`));
      if (hasCloudDump) console.log(chalk.gray(`Cloud Dump: ${options.cloudDump}`));
      if (hasEdgeUrl) console.log(chalk.gray(`Edge DB: ${options.edge}`));
      if (hasEdgeDump) console.log(chalk.gray(`Edge Dump: ${options.edgeDump}`));
      console.log(chalk.gray(`Schema: ${options.schema}`));
      if (options.execute) {
        console.log(chalk.red('⚠️  AUTOMATIC RECORD INSERTION MODE ACTIVE!'));
      }
      if (options.dryRun) {
        console.log(chalk.yellow('🔍 DRY RUN MODE ACTIVE!'));
      }
      console.log('');
      
      const result = await dataComparator.compareData(
        options.cloud,
        options.edge,
        options.schema,
        { 
          verbose: options.verbose,
          cloudDump: options.cloudDump,
          edgeDump: options.edgeDump
        }
      );
      
      // Display results
      displayDataResults(result, options.verbose);
      
      // Generate organized output
      const organizedOutput = await dataComparator.generateOrganizedOutput(result.insertQueries);
      console.log(chalk.green(`📁 Organized data output created:`));
      console.log(chalk.gray(`   • Cloud → Edge: ${organizedOutput.cloudToEdgeQueries} SQL files`));
      console.log(chalk.gray(`   • Edge → Cloud: ${organizedOutput.edgeToCloudQueries} SQL files`));
      console.log(chalk.gray(`   • Total records: ${organizedOutput.totalQueries}`));
      
      // Execute mode - insert records
      if (options.execute) {
        console.log(chalk.blue('\n🚀 Inserting missing records...'));
        
        const executionResults = await dataComparator.executeAllInsertQueries(
          result.insertQueries,
          options.cloud,
          options.edge,
          { dryRun: options.dryRun }
        );
        
        // Display execution results
        console.log(chalk.blue.bold('\n📋 Record Insertion Results:'));
        console.log(chalk.gray(`  • Total Successful: ${executionResults.cloud.success + executionResults.edge.success}`));
        console.log(chalk.gray(`  • Total Failed: ${executionResults.cloud.failed + executionResults.edge.failed}`));
        
        if (executionResults.cloud.success > 0 || executionResults.cloud.failed > 0) {
          console.log(chalk.blue('\n☁️ Cloud Database:'));
          console.log(chalk.gray(`    • Successful: ${executionResults.cloud.success}`));
          console.log(chalk.gray(`    • Failed: ${executionResults.cloud.failed}`));
        }
        
        if (executionResults.edge.success > 0 || executionResults.edge.failed > 0) {
          console.log(chalk.blue('\n🏢 Edge Database:'));
          console.log(chalk.gray(`    • Successful: ${executionResults.edge.success}`));
          console.log(chalk.gray(`    • Failed: ${executionResults.edge.failed}`));
        }
        
        // Display errors
        const allErrors = [...executionResults.cloud.errors, ...executionResults.edge.errors];
        if (allErrors.length > 0) {
          console.log(chalk.red.bold('\n❌ Errors:'));
          allErrors.forEach(error => {
            console.log(chalk.red(`  • ${error}`));
          });
        }
      }
      
    } catch (error) {
      console.error(chalk.red.bold('❌ Error:'), error.message);
      if (options.verbose) {
        console.error(chalk.red(error.stack));
      }
      process.exit(1);
    }
  });

program.parse();

function displayResults(result, verbose = false) {
  console.log(chalk.blue.bold('\n📋 Comparison Results'));
  console.log(chalk.gray('='.repeat(50)));
  
  // General statistics
  console.log(chalk.cyan.bold('📊 General Statistics:'));
  console.log(`  • Total tables (Cloud): ${result.sourceStats.totalTables}`);
  console.log(`  • Total tables (Edge): ${result.targetStats.totalTables}`);
  console.log(`  • Common tables: ${result.commonTables.length}`);
  console.log(`  • Only in Cloud: ${result.onlyInSource.length}`);
  console.log(`  • Only in Edge: ${result.onlyInTarget.length}`);
  
  // Missing table summary
  if (result.summary && result.summary.totalMissingTables > 0) {
    console.log(`  • Total missing tables: ${result.summary.totalMissingTables}`);
    console.log(`  • Missing in Edge: ${result.summary.missingInEdge} tables`);
    console.log(`  • Missing in Cloud: ${result.summary.missingInCloud} tables`);
  }
  console.log('');
  
  // Tables only in Cloud (missing in Edge)
  if (result.onlyInSource.length > 0) {
    console.log(chalk.yellow.bold('⚠️  Tables Only in Cloud Database (Missing in Edge):'));
    result.onlyInSource.forEach(table => {
      console.log(`  • ${chalk.yellow(table)}`);
    });
    console.log('');
  }
  
  // Tables only in Edge (missing in Cloud)
  if (result.onlyInTarget.length > 0) {
    console.log(chalk.blue.bold('ℹ️  Tables Only in Edge Database (Missing in Cloud):'));
    result.onlyInTarget.forEach(table => {
      console.log(`  • ${chalk.blue(table)}`);
    });
    console.log('');
  }
  
  // CREATE TABLE SQL information
  if (result.createTableQueries && result.createTableQueries.length > 0) {
    console.log(chalk.green.bold('🔧 Generated CREATE TABLE SQLs:'));
    const edgeQueries = result.createTableQueries.filter(q => q.type === 'CREATE_IN_EDGE');
    const cloudQueries = result.createTableQueries.filter(q => q.type === 'CREATE_IN_CLOUD');
    
    if (edgeQueries.length > 0) {
      console.log(`  To be created in Edge database (from Cloud):`);
      edgeQueries.forEach(query => {
        console.log(`    • ${query.tableName}`);
      });
    }
    
    if (cloudQueries.length > 0) {
      console.log(`  To be created in Cloud database (from Edge):`);
      cloudQueries.forEach(query => {
        console.log(`    • ${query.tableName}`);
      });
    }
    console.log('');
  }
  
  // Common table differences
  if (result.tableDifferences.length > 0) {
    console.log(chalk.red.bold('🔍 Differences in Common Tables:'));
    result.tableDifferences.forEach(diff => {
      console.log(`\n  📋 Table: ${chalk.bold(diff.tableName)}`);
      
      if (diff.columnDifferences.length > 0) {
        console.log('    🔹 Column Differences:');
        diff.columnDifferences.forEach(colDiff => {
          console.log(`      • ${colDiff.columnName}: ${colDiff.difference}`);
        });
      }
      
      if (diff.constraintDifferences.length > 0) {
        console.log('    🔹 Constraint Differences:');
        diff.constraintDifferences.forEach(constDiff => {
          console.log(`      • ${constDiff.constraintName}: ${constDiff.difference}`);
        });
      }
      
      if (diff.indexDifferences.length > 0) {
        console.log('    🔹 Index Differences:');
        diff.indexDifferences.forEach(indexDiff => {
          console.log(`      • ${indexDiff.indexName}: ${indexDiff.difference}`);
        });
      }
    });
  } else if (result.commonTables.length > 0) {
    console.log(chalk.green('✅ No differences found in common tables!'));
  }
  
  console.log('\n' + '='.repeat(50));
  console.log(chalk.green.bold('✨ Schema comparison completed!'));
}

function displayDataResults(result, verbose = false) {
  console.log(chalk.blue.bold('\n📋 Data Comparison Results'));
  console.log(chalk.gray('='.repeat(50)));
  
  // General statistics
  console.log(chalk.cyan.bold('📊 General Statistics:'));
  console.log(`  • Total tables: ${result.summary.totalTables}`);
  console.log(`  • Tables with differences: ${result.summary.tablesWithDifferences}`);
  console.log(`  • Total missing records: ${result.summary.totalMissingRecords}`);
  
  console.log(chalk.cyan.bold('\n📋 Table-by-Table Results:'));
  result.tableResults.forEach(tableResult => {
    if (tableResult.hasDifferences) {
      console.log(`  ❌ ${tableResult.tableName}`);
      console.log(`    • Cloud record count: ${tableResult.totalCloudRecords}`);
      console.log(`    • Edge record count: ${tableResult.totalEdgeRecords}`);
      console.log(`    • Missing in Edge: ${tableResult.missingInEdge.length} records`);
      console.log(`    • Missing in Cloud: ${tableResult.missingInCloud.length} records`);
    } else {
      console.log(`  ✅ ${tableResult.tableName}`);
      console.log(`    • Cloud record count: ${tableResult.totalCloudRecords}`);
      console.log(`    • Edge record count: ${tableResult.totalEdgeRecords}`);
    }
    console.log('');
  });

  if (result.summary.tablesWithDifferences === 0) {
    console.log(chalk.green('✅ All tables are synchronized!'));
  } else {
    console.log(chalk.green.bold('🔧 Generated INSERT SQLs:'));
    const edgeQueries = result.insertQueries.filter(q => q.type === 'INSERT_TO_EDGE');
    const cloudQueries = result.insertQueries.filter(q => q.type === 'INSERT_TO_CLOUD');
    
    if (edgeQueries.length > 0) {
      console.log(`  To be inserted into Edge database (from Cloud):`);
      const edgeTableCounts = {};
      edgeQueries.forEach(query => {
        edgeTableCounts[query.tableName] = (edgeTableCounts[query.tableName] || 0) + 1;
      });
      Object.entries(edgeTableCounts).forEach(([table, count]) => {
        console.log(`    • ${table}: ${count} records`);
      });
    }
    
    if (cloudQueries.length > 0) {
      console.log(`  To be inserted into Cloud database (from Edge):`);
      const cloudTableCounts = {};
      cloudQueries.forEach(query => {
        cloudTableCounts[query.tableName] = (cloudTableCounts[query.tableName] || 0) + 1;
      });
      Object.entries(cloudTableCounts).forEach(([table, count]) => {
        console.log(`    • ${table}: ${count} records`);
      });
    }
  }
  
  console.log('\n' + '='.repeat(50));
  console.log(chalk.green.bold('✨ Data comparison completed!'));
} 