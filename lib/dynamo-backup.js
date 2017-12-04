var _ = require('lodash');
var AWS = require('aws-sdk');
var events = require('events');
var moment = require('moment');
var dateRange = require('moment-range');
var path = require('path');
var async = require('async');
var util = require('util');

var Uploader = require('s3-streaming-upload').Uploader;

var ReadableStream = require('./readable-stream');

const READ_CAPACITY_SIZE = 4096; // 4KB
const SINGLE_SCAN_MAX_SIZE = 1048576; // 1MB
const ESTIMATED_1MB_WRITE_TIME = 0.035; // ~35ms for 1MB response (estimation)
const RESPONSE_PARSING_AVG_TIME = 0.3; // time is in milliseconds, 0.3ms is the average duration to parse response result
const STREAM_HIGH_WATERMARK = 16384; // 16KB of highWaterMark of stream for buffering

function DynamoBackup(options) {
    var params = {};
    options = options || {};
    this.excludedTables = options.excludedTables || [];
    this.includedTables = options.includedTables;
    this.tablesToBackup = [];
    this.consistentRead = !!options.consistentRead; // strongly consistent
    this.consistentRatio = this.consistentRead ? 1 : 0.5; // eventually consistent consumes 1/2 of a read capacity unit.
    this.singleCapacitySize = READ_CAPACITY_SIZE / this.consistentRatio; // 4KB for consistent, 8KB for eventually consistent
    this.maxSingleScanReadCapacity = SINGLE_SCAN_MAX_SIZE / this.singleCapacitySize; // 1MB per scan page
    this.readPercentage = options.readPercentage || .25;
    this.maxSecondsPerTable = 60 * (options.maxMinutesPerTable || Infinity);
    this.tablesCapacities = {};
    this.backupPath = options.backupPath;
    this.bucket = options.bucket;
    this.stopOnFailure = options.stopOnFailure || false;
    this.base64Binary = options.base64Binary || false;
    this.saveDataPipelineFormat = options.saveDataPipelineFormat || false;
    this.serverSideEncryption = options.serverSideEncryption || false
    this.acl = options.acl || false;
    this.awsAccessKey = options.awsAccessKey;
    this.awsSecretKey = options.awsSecretKey;
    this.awsRegion = options.awsRegion;
    this.debug = Boolean(options.debug);

    if (this.awsRegion) {
        params.region = this.awsRegion;
    }
    if (this.awsAccessKey && this.awsSecretKey) {
        params.accessKeyId = this.awsAccessKey;
        params.secretAccessKey = this.awsSecretKey;
    }

    AWS.config.update(params);
}

util.inherits(DynamoBackup, events.EventEmitter);

DynamoBackup.prototype.listTables = function (callback) {
    var self = this;
    self._fetchTables(null, [], callback);
};

DynamoBackup.prototype.backupTable = function (tableName, backupPath, callback) {
    var self = this;
    var stream = new ReadableStream();

    if (callback === undefined) {
        callback = backupPath;
        backupPath = self._getBackupPath();
    }

    var params = {
        objectParams: {}
    };
    if (self.awsRegion) {
        params.region = self.awsRegion;
    }
    if (self.awsAccessKey && self.awsSecretKey) {
        params.accessKey = self.awsAccessKey;
        params.secretKey = self.awsSecretKey;
    }
    if (self.serverSideEncryption) {
        params.objectParams.ServerSideEncryption = self.serverSideEncryption
    }
    if (self.acl) {
        params.objectParams.ACL = self.acl
    }

    params.bucket = self.bucket;
    params.objectName = path.join(backupPath, tableName + '.json');
    params.stream = stream;
    params.debug = self.debug;

    var upload = new Uploader(params);

    var startTime = moment.utc();
    self.emit('start-backup', tableName, startTime);
    upload.send(function (err) {
        if (err) {
            self.emit('error', tableName, err);
        }
        var endTime = moment.utc();
        var backupDuration = new dateRange(startTime, endTime);
        self.emit('end-backup', tableName, backupDuration);
        return callback(err);
    });

    self._copyTable(
        tableName,
        function (items) {
            items.forEach(function (item) {
                if (self.base64Binary) {
                    _.each(item, function (value, key) {
                        if (value && value.B) {
                            value.B = new Buffer(value.B).toString('base64');
                        }
                    });
                }

                if (self.saveDataPipelineFormat) {
                    stream.append(self._formatForDataPipeline(item));
                } else {
                    stream.append(JSON.stringify(item));
                }
                stream.append('\n');
            });
        },
        function (err) {
            stream.end();
            if (err) {
                self.emit('error', tableName, err);
            }
        }
        );
};

DynamoBackup.prototype.backupAllTables = function (callback) {
    var self = this;
    var backupPath = self._getBackupPath();

    self.listTables(function (err, tables) {
        if (err) {
            return callback(err);
        }
        var includedTables = self.includedTables || tables;
        tables = _.difference(tables, self.excludedTables);
        tables = _.intersection(tables, includedTables);
        self.tablesToBackup = tables;

        async.each(tables,
            function (tableName, done) {
                self.backupTable(tableName, backupPath, function (err) {
                    if (err) {
                        if (self.stopOnFailure) {
                            return done(err);
                        }
                    }
                    done();
                })
            },
            callback
            );
    });
};

DynamoBackup.prototype._getBackupPath = function () {
    var self = this;
    var now = moment.utc();
    return self.backupPath || ('DynamoDB-backup-' + now.format('YYYY-MM-DD-HH-mm-ss'));
};

DynamoBackup.prototype._copyTable = function (tableName, itemsReceived, callback) {
    var self = this;
    var ddb = new AWS.DynamoDB();
    var startDescTime = new Date().getTime();
    ddb.describeTable({ TableName: tableName }, function (err, data) {
        if (err) {
            return callback(err);
        }
        var descTime = new Date().getTime() - startDescTime - RESPONSE_PARSING_AVG_TIME;
        var descSize = this.httpResponse.stream.socket.bytesRead;
        var speed = 1000 * descSize / descTime;

        const Table = data.Table;
        var originalReadCapacity = Table.ProvisionedThroughput.ReadCapacityUnits;
        var originalWriteCapacity = Table.ProvisionedThroughput.WriteCapacityUnits; // needed for future update
        var readPercentage = self.readPercentage;
        var maxCapacityUnits = Math.max(Math.floor(originalReadCapacity * readPercentage), 1);
        var tableCapacityConsumption = Table.TableSizeBytes / self.singleCapacitySize;
        var averageItemSize = Math.ceil(Table.TableSizeBytes / Table.ItemCount);

        var minimumNbScans = Math.ceil(Table.TableSizeBytes / SINGLE_SCAN_MAX_SIZE); // 1 scan can't return more than 1MB of data
        var estimatedWriteTime = minimumNbScans * ESTIMATED_1MB_WRITE_TIME * self.tablesToBackup.length; // with tables concurrency

        // estimate the backup duration with the current read capacity
        var nbSecondsToBackup = Math.ceil(tableCapacityConsumption / maxCapacityUnits) + estimatedWriteTime;

        if( nbSecondsToBackup <= self.maxSecondsPerTable ) {
            self._streamItems(tableName, null, 0, maxCapacityUnits, averageItemSize, itemsReceived, callback);
            return;
        }

        var estimatedFetchRatio = speed / STREAM_HIGH_WATERMARK ; // estimation ratio: if >1: faster, <1: slower.
        var maxSingleScanReadCapacity = self.maxSingleScanReadCapacity * estimatedFetchRatio;

        // Calculate the best read capacity, depending of wanted backup time
        var newReadCapacity = tableCapacityConsumption / (self.maxSecondsPerTable - estimatedWriteTime);
        if( newReadCapacity > maxSingleScanReadCapacity ) {
            newReadCapacity = maxSingleScanReadCapacity;
            nbSecondsToBackup = Math.ceil(tableCapacityConsumption / newReadCapacity) + estimatedWriteTime;
            self.emit('warning', tableName, "Can't backup in wanted duration, estimated backup duration: " + Math.ceil(nbSecondsToBackup) + " seconds (±10%).");
        }

        // update new capacity and scan
        newReadCapacity = Math.ceil(newReadCapacity / readPercentage);
        self._updateTableCapacities(tableName, newReadCapacity, originalWriteCapacity, function(err, data){
            if (err) {
                if( err.code != 'ValidationException' && err.code != 'LimitExceededException' ) {
                    return callback(err);
                }
                self.emit('warning', tableName, "Can't increase the read capacity value from " + originalReadCapacity + " to " + newReadCapacity);
            }
            // save old and new capacity, to restore after backup
            self.tablesCapacities[tableName] = {
                oldRead: originalReadCapacity,
                newRead: newReadCapacity,
                oldWrite: originalWriteCapacity,
                newWrite: originalWriteCapacity,
            };
            var maxCapacityUnits = Math.max(Math.floor(newReadCapacity * readPercentage), 1);
            self._streamItems(tableName, null, 0, maxCapacityUnits, averageItemSize, itemsReceived, function(err) {
                self._updateTableOriginalCapacities(tableName, function(){
                    callback(err); // err is parent error
                });
            });
        });
    });
};

DynamoBackup.prototype._streamItems = function fetchItems(tableName, startKey, remainingCapacityUnits, maxCapacityUnits, averageItemSize, itemsReceived, callback) {
    var self = this;
    var ddb = new AWS.DynamoDB();
    var startOfSecond = new Date().getTime();
    
    var usableCapacity = maxCapacityUnits + remainingCapacityUnits;
    if (usableCapacity > 0) {
        var limit = Math.max(Math.floor((self.singleCapacitySize / averageItemSize) * usableCapacity), 1);
        var params = {
            Limit: limit,
            ConsistentRead: self.consistentRead,
            ReturnConsumedCapacity: 'TOTAL',
            TableName: tableName
        };

        if (startKey) {
            params.ExclusiveStartKey = startKey;
        }

        ddb.scan(params, function (error, data) {
            if (error) {
                callback(error);
                return;
            }

            if (data.Items.length > 0) {
                itemsReceived(data.Items);
            }


            if (!data.LastEvaluatedKey || _.keys(data.LastEvaluatedKey).length === 0) {
                callback();
                return;
            }

            var nextRemainingCapacity = usableCapacity - data.ConsumedCapacity.CapacityUnits;
            var duration = new Date().getTime() - startOfSecond;
            var timeout = 1000 - duration;
            if( timeout < 0 || (maxCapacityUnits*duration/1000) < nextRemainingCapacity) {
              // all time are not consumed, or there is too many remaining capacity
              nextRemainingCapacity -= maxCapacityUnits*timeout/1000;
              timeout = 0;
            }

            setTimeout(function () {
                self._streamItems(tableName, data.LastEvaluatedKey, Math.floor(nextRemainingCapacity), maxCapacityUnits, averageItemSize, itemsReceived, callback);
            }, timeout);
        });
    } else {
        // Wait until we have capacity again
        setTimeout(() => {
            self._streamItems(tableName, startKey, usableCapacity, maxCapacityUnits, averageItemSize, itemsReceived, callback);
        }, 1000);
    }
};

DynamoBackup.prototype._fetchTables = function (lastTable, tables, callback) {
    var self = this;
    var ddb = new AWS.DynamoDB();
    var params = {};
    if (lastTable) {
        params.ExclusiveStartTableName = lastTable;
    }
    ddb.listTables(params, function (err, data) {
        if (err) {
            return callback(err, null);
        }
        tables = tables.concat(data.TableNames);
        if (data.LastEvaluatedTableName) {
            self._fetchTables(data.LastEvaluatedTableName, tables, callback);
        } else {
            callback(null, tables);
        }
    });
};

/**
 * AWS Data Pipeline import requires that each key in the Attribute list
 * be lower-cased and for sets start with a lower-case character followed
 * by an 'S'.
 * 
 * Go through each attribute and create a new entry with the correct case
 */
DynamoBackup.prototype._formatForDataPipeline = function (item) {
    var self = this;
    _.each(item, function (value, key) {
        //value will be of the form: {S: 'xxx'}. Convert the key
        _.each(value, function (v, k) {
            var dataPipelineValueKey = self._getDataPipelineAttributeValueKey(k);
            value[dataPipelineValueKey] = v;
            value[k] = undefined;
            // for MAps and Lists, recurse until the elements are created with the correct case
            if(k === 'M' || k === 'L') {
              self._formatForDataPipeline(v);
            }
        });
    });
    return JSON.stringify(item);
};

DynamoBackup.prototype._getDataPipelineAttributeValueKey = function (type) {
    switch (type) {
        case 'S':
        case 'N':
        case 'B':
        case 'M':
        case 'L':
        case 'NULL':
            return type.toLowerCase();
        case 'BOOL':
            return 'bOOL';
        case 'SS':
            return 'sS';
        case 'NS':
            return 'nS';
        case 'BS':
            return 'bS';
        default:
            throw new Error('Unknown AttributeValue key: ' + type);
    }
};

DynamoBackup.prototype._updateTableCapacities = function(tableName, newReadCapacity, newWriteCapacity, callback) {
    var params = {
        TableName: tableName,
        ProvisionedThroughput: {
            ReadCapacityUnits: newReadCapacity,
            WriteCapacityUnits: newWriteCapacity
        }
    };

    var ddb = new AWS.DynamoDB();
    ddb.updateTable(params, callback);
};

DynamoBackup.prototype._updateTableOriginalCapacities = function(tableName, callback) {
    var self = this;
    if( !self.tablesCapacities[tableName] ) {
      return callback && callback();
    }
    var originalReadCapacity = self.tablesCapacities[tableName].oldRead;
    var currentReadCapacity = self.tablesCapacities[tableName].newRead;
    var diffReadApplied = currentReadCapacity - originalReadCapacity;

    var originalWriteCapacity = self.tablesCapacities[tableName].oldWrite;
    var currentWriteCapacity = self.tablesCapacities[tableName].newWrite;
    var diffWriteApplied = currentWriteCapacity - originalWriteCapacity;

    var ddb = new AWS.DynamoDB();
    ddb.describeTable({ TableName: tableName }, function (err, data) {
        if (!err) {
            currentReadCapacity = data.Table.ProvisionedThroughput.ReadCapacityUnits;
            currentWriteCapacity = data.Table.ProvisionedThroughput.WriteCapacityUnits;
        }
        var newReadCapacity = Math.max(currentReadCapacity - diffReadApplied, originalReadCapacity);
        var newWriteCapacity = Math.max(currentWriteCapacity - diffWriteApplied, originalWriteCapacity);

        if( newReadCapacity == currentReadCapacity && newWriteCapacity == currentWriteCapacity ) {
            return callback && callback();
        }

        self._updateTableCapacities(tableName, newReadCapacity, newWriteCapacity, function(err, data){
            if(err && err.code == 'LimitExceededException') {
                self.emit('warning', tableName, "Can't decrease the read capacity value " + currentReadCapacity + " to " + newReadCapacity);
            }
            return callback && callback();
        });
    });
};

DynamoBackup.prototype.restoreOriginalCapacities = function(callback) {
    var self = this;
    var tables = Object.keys(this.tablesCapacities);
    if( 0 == tables.length ) {
        return callback && callback();
    }
    this._updateTableOriginalCapacities(tables[0], function(){
      delete self.tablesCapacities[tables[0]];
      return self.restoreOriginalCapacities(callback);
    });
}

module.exports = DynamoBackup;

