
// Adds properties of obj2 into obj1
function merge(obj1, obj2) {
  var obj3 = {};
    for (var atr in obj1) { obj3[atr] = obj1[atr]; }
    for (var atr in obj2) { obj3[atr] = obj2[atr]; }
    return obj3;
}


var FQL = function(table) {
    this.table = table;
    this.index = [];
};

FQL.prototype.exec = function () {
    return this.table;
};


FQL.prototype.count = function () {
    return this.table.length;
};

FQL.prototype.limit = function (amount) {
    this.table = this.table.slice(0, amount);
    return this;
};

FQL.prototype.where = function (filters) {
  // for ever key/value pair
  // filter out non-matches
  // values may be functions or primitives
  // functions should be called on each row value
  var newTable = this.exec();
  for (var colName in filters) {
    var valOrFn = filters[colName];
    var indexed = false;
    for(var a = 0, len = this.index.length; a < len ; a++){
          if(colName == this.index[a].name){
            indexed = true;
          }
    }

    if (valOrFn instanceof Function) {
      newTable = newTable.filter(function (row) {
        return valOrFn(row[colName]);
      });
    } 

    else if ( indexed  ){
      var results= [];
      var indeces = this.getIndicesOf(colName, filters[colName]);
      for(var b = 0, len = indeces.length; b<len; b++){
        results.push(newTable[indeces[b]]);
      }
      newTable = results;
    } 

    else {
      newTable = newTable.filter(function (row) {
        return valOrFn == row[colName];
      });
    }
  }
  return new FQL(newTable);
};

FQL.prototype.select = function (columnNames) {
  var newTable = this.exec().map(function (row) {
    var newRow = {};
    columnNames.forEach(function (colName) {
      newRow[colName] = row[colName];
    });
    return newRow;
  });
  return new FQL(newTable);
};

FQL.prototype.order = function (columnName) {
  // a lot like sorting
  var newTable = this.exec().sort(function (currRow, nextRow) {
    return currRow[columnName] - nextRow[columnName];
  });
  return new FQL(newTable);
};

FQL.prototype.nimit = function () {};

FQL.prototype.left_join = function (foreignFql, rowMatcher) {

  //get both tables
  //add table to old table where the ids match //need to hard code ids? double for loop?
  //return new table

  var table = this.exec();
  var newTable = [];
  table.forEach(function(tr){
        foreignFql.exec().forEach(function(trj){
            if(rowMatcher(tr, trj)){
              newTable.push(merge(tr, trj));
            }
        })
  })
  return new FQL(newTable);

};

FQL.prototype.addIndex = function (columnName) {
    var table = this.exec();
    var indexer = { name: columnName, index: []};
    a=0;
    table.forEach(function(row){
      indexer.index[a++] = row[columnName]; 
    })
    this.index.push(indexer);
};

FQL.prototype.getIndicesOf = function (columnName, val) {
  var ind = this.index 
  var loc = null;
  var results = [];
  for(var a=0, len = ind.length; a< len; a++){
    if(ind[a].name === columnName){
      loc = a;
    }
  }
  if(loc == null){
    return undefined;
  }

  for(var b= 0, len = ind[loc].index.length; b < len; b++){
    if(ind[loc].index[b] === val){
      results.push(b);
    }
  }
  return results;

};
