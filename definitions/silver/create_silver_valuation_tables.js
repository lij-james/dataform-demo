// --- CONFIGURATION ---
const project = "colliers-apc-plafrm-box1";
const sourceTable = "colliers-apc-plafrm-box1.bronze_val.bronze_single_row";
const targetDataset = "colliers-apc-plafrm-box1.silver_val";

// The operate block runs custom multi-statement SQL in BigQuery
operate("valuation_silver_refresh").queries(ctx => `
  BEGIN
    -- 1. PARSE JSON: Read the single, flattened row directly
    DECLARE full_json JSON;
    SET full_json = (SELECT PARSE_JSON(json_content) FROM \`${sourceTable}\`);

    -- ==========================================
    -- SECTION 1: CORE BUILDING DATA (1:1)
    -- ==========================================

    -- TABLE 1: attributes
    CREATE OR REPLACE TABLE \`${targetDataset}.attributes\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(full_json.attributes.modelName) as modelName,
      JSON_VALUE(full_json.attributes.type) as type,
      JSON_VALUE(full_json.attributes.createdOn) as createdOn,
      JSON_VALUE(full_json.attributes.createdByEmail) as createdByEmail,
      JSON_VALUE(full_json.attributes.versionCreated) as versionCreated;

    -- TABLE 2: property
    CREATE OR REPLACE TABLE \`${targetDataset}.property\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(full_json.property.id) as id,
      JSON_VALUE(full_json.property.fullAddress) as fullAddress,
      JSON_VALUE(full_json.property.streetNumber) as streetNumber,
      JSON_VALUE(full_json.property.streetName) as streetName,
      JSON_VALUE(full_json.property.suburb) as suburb,
      JSON_VALUE(full_json.property.city) as city,
      JSON_VALUE(full_json.property.state) as state,
      JSON_VALUE(full_json.property.postCode) as postCode;

    -- TABLE 3: reporting
    CREATE OR REPLACE TABLE \`${targetDataset}.reporting\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(full_json.inputs.reporting.proprietor) as proprietor,
      SAFE_CAST(JSON_VALUE(full_json.inputs.reporting.siteArea) AS FLOAT64) as siteArea,
      JSON_VALUE(full_json.inputs.reporting.grade) as grade,
      JSON_VALUE(full_json.inputs.reporting.officeFloors) as officeFloors,
      JSON_VALUE(full_json.inputs.reporting.yearBuilt) as yearBuilt,
      JSON_VALUE(full_json.inputs.reporting.primaryValuer) as primaryValuer,
      JSON_VALUE(full_json.inputs.reporting.secondaryValuer) as secondaryValuer;

    -- TABLE 4: inputs_general
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_general\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(full_json.inputs.general.valuationDate) as valuationDate,
      JSON_VALUE(full_json.inputs.general.valuationType) as valuationType,
      JSON_VALUE(full_json.inputs.general.valuationMethod) as valuationMethod,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.roundedAdoptedValue) AS FLOAT64) as roundedAdoptedValue,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.discountRate) AS FLOAT64) as discountRate,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.capRateTerminal) AS FLOAT64) as capRateTerminal;

    -- TABLE 5: outputs_summary (Master Performance Metrics)
    CREATE OR REPLACE TABLE \`${targetDataset}.outputs_summary\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.roundedAdoptedValuation) AS FLOAT64) as roundedAdoptedValuation,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.initialPassingYield) AS FLOAT64) as initialPassingYield,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.tenYearIRR) AS FLOAT64) as tenYearIRR,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.capitalValuePsqm) AS FLOAT64) as capitalValuePsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.grossPassingIncomePA) AS FLOAT64) as grossPassingIncomePA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.netPassingIncomePA) AS FLOAT64) as netPassingIncomePA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.netPresentValue) AS FLOAT64) as netPresentValue;

    -- ==========================================
    -- SECTION 2: GRANULAR TENANT & EXPENSE DATA (1:MANY)
    -- ==========================================

    -- TABLE 6: inputs_spaces (Core Lease Data)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_spaces\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(s.level) as level,
      JSON_VALUE(s.suite) as suite,
      JSON_VALUE(s.lease.tenantName) as tenantName,
      JSON_VALUE(s.rentBasis) as rentBasis,
      SAFE_CAST(JSON_VALUE(s.lettableArea) AS FLOAT64) as lettableArea,
      SAFE_CAST(JSON_VALUE(s.lease.baseRent) AS FLOAT64) as baseRent,
      JSON_VALUE(s.lease.startDate) as startDate,
      JSON_VALUE(s.lease.expiryDate) as expiryDate,
      JSON_VALUE(s.lease.options) as options,
      JSON_VALUE(s.renewalAssumptionName) as renewalAssumptionName
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.spaces)) AS s
    WHERE JSON_VALUE(s.lease.tenantName) IS NOT NULL AND JSON_VALUE(s.lease.tenantName) != '-';

    -- TABLE 7: inputs_incentives (Financial Concessions)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_incentives\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(s.lease.tenantName) as tenantName,
      JSON_VALUE(s.level) as level,
      SAFE_CAST(JSON_VALUE(s.lease.commencementIncentives.total) AS FLOAT64) as commencementTotal,
      JSON_VALUE(s.lease.commencementIncentives.type) as commencementType,
      JSON_VALUE(s.lease.commencementIncentives.incentiveAmortisation.startDate) as amortisationStartDate,
      SAFE_CAST(JSON_VALUE(s.lease.outstandingIncentives.lumpSumIncentives.amount) AS FLOAT64) as outstandingLumpSum,
      SAFE_CAST(JSON_VALUE(s.lease.outstandingIncentives.rentFreeIncentives.percentApplied) AS FLOAT64) as rentFreePercent
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.spaces)) AS s
    WHERE JSON_VALUE(s.lease.tenantName) IS NOT NULL AND JSON_VALUE(s.lease.tenantName) != '-';

    -- TABLE 8: inputs_outgoings (Line-by-Line Expenses)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_outgoings\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(item.name) as name,
      JSON_VALUE(item.type) as type,
      JSON_VALUE(item.pcaCategory) as pcaCategory,
      SAFE_CAST(JSON_VALUE(item.previousBudget) AS FLOAT64) as previousBudget,
      SAFE_CAST(JSON_VALUE(item.currentBudget) AS FLOAT64) as currentBudget,
      SAFE_CAST(JSON_VALUE(item.adoptedBudget) AS FLOAT64) as adoptedBudget
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.outgoings.items)) AS item;

    -- ==========================================
    -- SECTION 3: TIME-SERIES & FORECASTING DATA (1:MANY ARRAYS)
    -- ==========================================

    -- TABLE 9: inputs_capex (Unrolled 10-Year Capital Expenditure)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_capex\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(capex.name) as name,
      JSON_VALUE(capex.category) as category,
      JSON_VALUE(capex.type) as type,
      year_offset as year_index,
      SAFE_CAST(JSON_VALUE(amount) AS FLOAT64) as forecastAmount
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.capex.budgetedCapexs)) AS capex,
         UNNEST(JSON_QUERY_ARRAY(capex.amounts)) AS amount WITH OFFSET as year_offset;

    -- TABLE 10: inputs_escalations (Unrolled 10-Year CPI & Outgoings Growth)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_escalations\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(esc.name) as categoryName,
      SAFE_CAST(JSON_VALUE(esc.increaseOnCpi) AS FLOAT64) as cpiPremium,
      year_offset as year_index,
      SAFE_CAST(JSON_VALUE(rate) AS FLOAT64) as escalationRate
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.futures.escalationAssumptions)) AS esc,
         UNNEST(JSON_QUERY_ARRAY(esc.rates)) AS rate WITH OFFSET as year_offset;

    -- TABLE 11: inputs_market_growth (Unrolled 10-Year Rent Growth by Category)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_market_growth\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(mkt.name) as categoryName,
      year_offset as year_index,
      SAFE_CAST(JSON_VALUE(rate) AS FLOAT64) as marketGrowthRate
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.futures.marketGrowthAssumptions)) AS mkt,
         UNNEST(JSON_QUERY_ARRAY(mkt.rates)) AS rate WITH OFFSET as year_offset;

    -- TABLE 12: outputs_category_analysis (Performance split by Retail/Office/Storage)
    CREATE OR REPLACE TABLE \`${targetDataset}.outputs_category_analysis\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(renewal.name) as categoryName,
      SAFE_CAST(JSON_VALUE(renewal.averageGrossPassingPsqm) AS FLOAT64) as avgGrossPassingPsqm,
      SAFE_CAST(JSON_VALUE(renewal.averageNetPassingPsqm) AS FLOAT64) as avgNetPassingPsqm,
      SAFE_CAST(JSON_VALUE(renewal.averageNetMarketPsqm) AS FLOAT64) as avgNetMarketPsqm
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.outputs.incomeAnalysis.renewalTypeIncomes)) AS renewal;

  END;
`);