#include <stdio.h>
#include <stdlib.h>
#include <string.h> /*strtok(),strcpy()*/

#include "Parser.h"
#include "Utils.h"
#include "Relation.h"
#include "Optimizer.h"


void createQueryEstimations(struct QueryInfo *qInfo,struct Joiner *j)
{
	qInfo->estimations = malloc(qInfo->numOfRelationIds*sizeof(struct columnStats*));
	MALLOC_CHECK(qInfo->estimations);
	for(unsigned i=0;i<qInfo->numOfRelationIds;++i)
	{
		unsigned relId = qInfo->relationIds[i];
		unsigned size = j->relations[relId]->numOfCols*sizeof(struct columnStats);

		// Allocate space to store the estimations
		qInfo->estimations[i] = malloc(size);
		MALLOC_CHECK(qInfo->estimations[i]);
		// Fetch the stats, calculated when the relations were being loaded in memory
		memcpy(qInfo->estimations[i],j->relations[relId]->stats,size);
	}
}

void createQueryInfo(struct QueryInfo **qInfo,char *rawQuery)
{
	*qInfo = malloc(sizeof(struct QueryInfo));
	MALLOC_CHECK(*qInfo);
	(*qInfo)->estimations = NULL;
	parseQuery(*qInfo,rawQuery);
}

void destroyQueryInfo(struct QueryInfo *qInfo)
{
	free(qInfo->relationIds);
	free(qInfo->predicates);
	free(qInfo->filters);
	free(qInfo->selections);
	if(qInfo->estimations)/* Useful in unit testing */
		for(unsigned i=0;i<qInfo->numOfRelationIds;++i)
			free(qInfo->estimations[i]);
	free(qInfo->estimations);
	free(qInfo);
}

void parseQuery(struct QueryInfo *qInfo,char *rawQuery)
{
	char rawRelations[BUFFERSIZE];
	char rawPredicates[BUFFERSIZE];
	char rawSelections[BUFFERSIZE];

	/* Split query into three parts */
	if( (sscanf(rawQuery,"%[^|]|%[^|]|%[^|]",rawRelations,rawPredicates,rawSelections)) != 3 )
	{
		fprintf(stderr,"Query \"%s\" does not consist of three parts\nExiting...\n\n",rawQuery);
		exit(EXIT_FAILURE);
	}

	/* Parse each part */
	parseRelationIds(qInfo,rawRelations);
	parsePredicates(qInfo,rawPredicates);
	parseSelections(qInfo,rawSelections);
}

void parseRelationIds(struct QueryInfo *qInfo,char *rawRelations)
{
	char* temp = rawRelations;
	unsigned i;
	int offset;

	/* Get number of relationIds */
	qInfo->numOfRelationIds = 0;
	while(sscanf(temp,"%*d%n",&offset)>=0)
	{
		++qInfo->numOfRelationIds;
		temp+=offset;
	}
	if(!qInfo->numOfRelationIds)
	{
		fprintf(stderr,"Zero join relations were found in the query\nExiting...\n\n");
		exit(EXIT_FAILURE);
	}

	/* Allocate memory for relationIds */
	qInfo->relationIds = malloc(qInfo->numOfRelationIds*sizeof(unsigned));
	MALLOC_CHECK(qInfo->relationIds);

	/* Store relationIds */
	temp = rawRelations;
	i=0;
	while(sscanf(temp,"%u%n",&qInfo->relationIds[i],&offset)>0)
	{
		++i;
		temp+=offset;
	}
}

void parseSelections(struct QueryInfo *qInfo,char *rawSelections)
{

	char* temp = rawSelections;
	unsigned relId,colId,i;
	int offset;

	/* Get number of selections */
	qInfo->numOfSelections = 0;
	while(sscanf(temp,"%*u.%*u%n",&offset)>=0)
	{
		++qInfo->numOfSelections;
		temp+=offset;
	}
	if(!qInfo->numOfSelections)
	{
		fprintf(stderr,"Zero selections were found in the query\nExiting...\n\n");
		exit(EXIT_FAILURE);
	}

	/* Allocate memory for selections */
	qInfo->selections = malloc(qInfo->numOfSelections*sizeof(struct SelectInfo));
	MALLOC_CHECK(qInfo->selections);

	/*  Store selections */
	temp = rawSelections;
	i=0;
	while(sscanf(temp,"%u.%u%n",&relId,&colId,&offset)>0)
	{
		qInfo->selections[i].relId = relId;
		qInfo->selections[i].colId = colId;
		++i;
		temp+=offset;
	}
}

void parsePredicates(struct QueryInfo *qInfo,char *rawPredicates)
{
	unsigned i,j;
	char *token;
	char *temp = malloc((strlen(rawPredicates)+1)*sizeof(char));
	MALLOC_CHECK(temp);
	strcpy(temp,rawPredicates);

	/* Get number of predicates and filters */
	qInfo->numOfFilters    = 0;
	qInfo->numOfPredicates = 0;
	token = strtok(temp,"&");
	if(isFilter(token))
		++qInfo->numOfFilters;
	else
		++qInfo->numOfPredicates;
	while(token=strtok(NULL,"&"))
		if(isFilter(token))
			++qInfo->numOfFilters;
		else
			++qInfo->numOfPredicates;

	if(!(qInfo->numOfPredicates+qInfo->numOfFilters))
	{
		fprintf(stderr,"Zero predicates were found in the query\nExiting...\n\n");
		exit(EXIT_FAILURE);
	}

	/* Allocate memory for predicates and filters */
	qInfo->predicates = malloc(qInfo->numOfPredicates*sizeof(struct PredicateInfo));
	MALLOC_CHECK(qInfo->predicates);
	qInfo->filters    = malloc(qInfo->numOfFilters*sizeof(struct FilterInfo));
	MALLOC_CHECK(qInfo->filters);

	/* Store predicates & filters */
	strcpy(temp,rawPredicates);
	token = strtok(temp,"&");
	i=j=0;
	if(isFilter(token))
		{addFilter(&qInfo->filters[i],token);++i;}
	else
		{addPredicate(&qInfo->predicates[j],token);++j;}


	while(token=strtok(NULL,"&"))
		if(isFilter(token))
			{addFilter(&qInfo->filters[i],token);++i;}
		else
			{addPredicate(&qInfo->predicates[j],token);++j;}

	free(temp);
}

void addFilter(struct FilterInfo *fInfo,char *token)
{
	unsigned relId;
	unsigned colId;
	char cmp;
	uint64_t constant;
	sscanf(token,"%u.%u%c%lu",&relId,&colId,&cmp,&constant);
	// printf("\"%u.%u%c%lu\"\n",relId,colId,cmp,constant);
	fInfo->filterLhs.relId = relId;
	fInfo->filterLhs.colId = colId;
	fInfo->comparison      = cmp;
	fInfo->constant        = constant;
}

void addPredicate(struct PredicateInfo *pInfo,char *token)
{
	unsigned relId1;
	unsigned colId1;
	unsigned relId2;
	unsigned colId2;
	sscanf(token,"%u.%u=%u.%u",&relId1,&colId1,&relId2,&colId2);
	// printf("\"%u.%u=%u.%u\"\n",relId1,colId1,relId2,colId2);
	pInfo->left.relId  = relId1;
	pInfo->left.colId  = colId1;
	pInfo->right.relId = relId2;
	pInfo->right.colId = colId2;
}
/*
我能看懂这个函数。这是一个**判断谓词类型**的函数，用于区分是**过滤条件**还是**连接条件**。

## 函数功能详解

### 函数原型：
```c
int isFilter(char *predicate)
```

### 作用：
判断给定的SQL谓词是**过滤条件**（Filter）还是**连接条件**（Join）

## 代码逐行解析

### 1. **解析谓词字符串**
```c
sscanf(predicate,"%*u.%*u%*[=<>]%s",constant);
```
- `%*u.%*u`：跳过"数字.数字"格式（如：`1.2`，表示第1个关系的第2列）
- `%*[=<>]`：跳过操作符（=, <, >）
- `%s`：读取操作符后面的常量值到 `constant` 数组

### 2. **判断条件类型**
```c
if(!strstr(constant,"."))
    return 1;  // 过滤条件
else
    return 0;  // 连接条件
```
- 如果常量中**不包含点号** → 是过滤条件
- 如果常量中**包含点号** → 是连接条件

## 示例说明

### 过滤条件示例：
```c
char *filterPredicate = "1.2=100";  // 第1个关系的第2列等于100
isFilter(filterPredicate);          // 返回 1
```
解析过程：
- 跳过 `1.2` 和 `=`
- 读取 `100` 到 constant
- `100` 中没有点号 → 是过滤条件

### 连接条件示例：
```c
char *joinPredicate = "1.2=2.1";    // 第1个关系的第2列 = 第2个关系的第1列  
isFilter(joinPredicate);            // 返回 0
```
解析过程：
- 跳过 `1.2` 和 `=`
- 读取 `2.1` 到 constant
- `2.1` 中包含点号 → 是连接条件

## 在查询处理中的用途

这个函数通常在**查询优化器**或**执行引擎**中使用：

```c
// 处理WHERE子句中的多个条件
for (int i = 0; i < numPredicates; i++) {
    if (isFilter(predicates[i])) {
        // 处理过滤条件：1.2=100
        applyFilter(relation, predicates[i]);
    } else {
        // 处理连接条件：1.2=2.1  
        applyJoin(relation1, relation2, predicates[i]);
    }
}
```

## 潜在问题和改进

### 1. **缓冲区溢出风险**
```c
char constant[20];  // 固定大小，可能溢出
```
改进方案：
```c
char constant[256];  // 使用更大缓冲区
// 或者使用安全的字符串处理函数
```

### 2. **格式兼容性**
当前只支持 `X.Y=Z` 或 `X.Y=W.Z` 格式，可能需要支持更多操作符和格式。

### 3. **错误处理**
没有检查 `sscanf` 的返回值，如果输入格式不匹配会出错。

## 总结

这个 `isFilter` 函数是一个实用的工具函数，通过分析谓词字符串的格式来区分：
- **返回1**：过滤条件（比较列和常量值）
- **返回0**：连接条件（比较两个列的值）

这种区分对于查询执行计划的生成和优化非常重要，因为过滤条件和连接条件需要不同的处理策略和执行算法。
*/
int isFilter(char *predicate)
{
	char constant[20];
	sscanf(predicate,"%*u.%*u%*[=<>]%s",constant);

	if(!strstr(constant,"."))
		return 1;
	else
		return 0;
}

int isColEquality(struct PredicateInfo *pInfo)
{return (pInfo->left.relId == pInfo->right.relId); }

unsigned getRelId(struct SelectInfo *sInfo)
{return sInfo->relId;}

unsigned getOriginalRelId(struct QueryInfo *qInfo,struct SelectInfo *sInfo)
{return qInfo->relationIds[sInfo->relId];}

unsigned getColId(struct SelectInfo *sInfo)
{return sInfo->colId;}

uint64_t getConstant(struct FilterInfo *fInfo)
{return fInfo->constant;}

Comparison getComparison(struct FilterInfo *fInfo)
{return fInfo->comparison;}

unsigned getNumOfFilters(struct QueryInfo *qInfo)
{return qInfo->numOfFilters;}

unsigned getNumOfRelations(struct QueryInfo *qInfo)
{return qInfo->numOfRelationIds;}

unsigned getNumOfColEqualities(struct QueryInfo *qInfo)
{
	unsigned sum=0;
	for(unsigned i=0;i<qInfo->numOfPredicates;++i)
		if(isColEquality(&qInfo->predicates[i]))
			++sum;
	return sum;
}

unsigned getNumOfJoins(struct QueryInfo *qInfo)
{
	unsigned sum=0;
	for(unsigned i=0;i<qInfo->numOfPredicates;++i)
		if(!isColEquality(&qInfo->predicates[i]))
			++sum;
	return sum;
}

/**************************** For Testing... ***************************************/
void printTest(struct QueryInfo *qInfo)
{
	for(unsigned j=0;j<qInfo->numOfRelationIds;++j)
	{
		fprintf(stderr,"%u ",qInfo->relationIds[j]);
	}
	fprintf(stderr,"|");
	for(unsigned j=0;j<qInfo->numOfPredicates;++j)
	{
		unsigned leftRelId  = getRelId(&qInfo->predicates[j].left);
		unsigned rightRelId = getRelId(&qInfo->predicates[j].right);
		unsigned leftColId  = getColId(&qInfo->predicates[j].left);
		unsigned rightColId = getColId(&qInfo->predicates[j].right);

		if(isColEquality(&qInfo->predicates[j]))
			fprintf(stderr,"[%u.%u=%u.%u] & ",leftRelId,leftColId,rightRelId,rightColId);
		else
			fprintf(stderr,"%u.%u=%u.%u & ",leftRelId,leftColId,rightRelId,rightColId);
	}
	for(unsigned j=0;j<qInfo->numOfFilters;++j)
	{
		unsigned relId    = getRelId(&qInfo->filters[j].filterLhs);
		unsigned colId    = getColId(&qInfo->filters[j].filterLhs);
		Comparison cmp    = getComparison(&qInfo->filters[j]);
		uint64_t constant = getConstant(&qInfo->filters[j]);

		fprintf(stderr,"%u.%u%c%ld & ",relId,colId,cmp,constant);
	}
	fprintf(stderr,"|");
	for(unsigned j=0;j<qInfo->numOfSelections;++j)
	{
		unsigned relId = getRelId(&qInfo->selections[j]);
		unsigned colId = getColId(&qInfo->selections[j]);
		fprintf(stderr,"%u.%u ",relId,colId);
	}
	fprintf(stderr, "\n");
}
