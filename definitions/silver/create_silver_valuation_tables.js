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
    -- SECTION 1: CORE 1:1 METADATA & INPUTS
    -- ==========================================

    -- TABLE 1: attributes (100% Captured)
    CREATE OR REPLACE TABLE \`${targetDataset}.attributes\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(full_json.attributes.modelName) as modelName,
      JSON_VALUE(full_json.attributes.type) as type,
      JSON_VALUE(full_json.attributes.createdOn) as createdOn,
      JSON_VALUE(full_json.attributes.createdByEmail) as createdByEmail,
      JSON_VALUE(full_json.attributes.teamId) as teamId,
      JSON_VALUE(full_json.attributes.externalId) as externalId,
      JSON_VALUE(full_json.attributes.productCreated) as productCreated,
      JSON_VALUE(full_json.attributes.versionCreated) as versionCreated;

    -- TABLE 2: property (100% Captured)
    CREATE OR REPLACE TABLE \`${targetDataset}.property\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(full_json.property.id) as id,
      JSON_VALUE(full_json.property.name) as name,
      JSON_VALUE(full_json.property.fullAddress) as fullAddress,
      JSON_VALUE(full_json.property.streetNumber) as streetNumber,
      JSON_VALUE(full_json.property.streetName) as streetName,
      JSON_VALUE(full_json.property.suburb) as suburb,
      JSON_VALUE(full_json.property.city) as city,
      JSON_VALUE(full_json.property.state) as state,
      JSON_VALUE(full_json.property.postCode) as postCode,
      JSON_VALUE(full_json.property.country) as country;

    -- TABLE 3: inputs_reporting (100% Captured)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_reporting\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(full_json.inputs.reporting.proprietor) as proprietor,
      SAFE_CAST(JSON_VALUE(full_json.inputs.reporting.siteArea) AS FLOAT64) as siteArea,
      JSON_VALUE(full_json.inputs.reporting.legalDescription) as legalDescription,
      JSON_VALUE(full_json.inputs.reporting.localGovernmentArea) as localGovernmentArea,
      JSON_VALUE(full_json.inputs.reporting.zoning) as zoning,
      SAFE_CAST(JSON_VALUE(full_json.inputs.reporting.floorSpaceRatio) AS FLOAT64) as floorSpaceRatio,
      JSON_VALUE(full_json.inputs.reporting.planningScheme) as planningScheme,
      SAFE_CAST(JSON_VALUE(full_json.inputs.reporting.nabersEnergy) AS FLOAT64) as nabersEnergy,
      SAFE_CAST(JSON_VALUE(full_json.inputs.reporting.nabersWater) AS FLOAT64) as nabersWater,
      JSON_VALUE(full_json.inputs.reporting.yearBuilt) as yearBuilt,
      JSON_VALUE(full_json.inputs.reporting.lastRefurbYear) as lastRefurbYear,
      JSON_VALUE(full_json.inputs.reporting.sector) as sector,
      JSON_VALUE(full_json.inputs.reporting.subSector) as subSector,
      JSON_VALUE(full_json.inputs.reporting.precinct) as precinct,
      JSON_VALUE(full_json.inputs.reporting.grade) as grade,
      JSON_VALUE(full_json.inputs.reporting.officeFloors) as officeFloors,
      JSON_VALUE(full_json.inputs.reporting.endOfTrip) as endOfTrip,
      JSON_VALUE(full_json.inputs.reporting.pcaMarket) as pcaMarket,
      JSON_VALUE(full_json.inputs.reporting.pcaCharacteristic) as pcaCharacteristic,
      JSON_VALUE(full_json.inputs.reporting.areaLabels) as areaLabels,
      JSON_VALUE(full_json.inputs.reporting.title) as title,
      JSON_VALUE(full_json.inputs.reporting.company) as company,
      JSON_VALUE(full_json.inputs.reporting.jobReference) as jobReference,
      JSON_VALUE(full_json.inputs.reporting.purpose) as purpose,
      JSON_VALUE(full_json.inputs.reporting.primaryValuer) as primaryValuer,
      JSON_VALUE(full_json.inputs.reporting.secondaryValuer) as secondaryValuer;

    -- TABLE 4: inputs_general (100% Captured + Base Future Settings)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_general\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(full_json.inputs.general.valuationDate) as valuationDate,
      JSON_VALUE(full_json.inputs.general.valuationType) as valuationType,
      JSON_VALUE(full_json.inputs.general.valuationMethod) as valuationMethod,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.roundedAdoptedValue) AS FLOAT64) as roundedAdoptedValue,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.adoptedValueAdjustment) AS FLOAT64) as adoptedValueAdjustment,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.capRateInitial) AS FLOAT64) as capRateInitial,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.capRateMarket) AS FLOAT64) as capRateMarket,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.capRateTerminalBps) AS FLOAT64) as capRateTerminalBps,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.interestValued) AS FLOAT64) as interestValued,
      JSON_VALUE(full_json.inputs.general.incentiveBasis) as incentiveBasis,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.expiryAllowanceMonths) AS FLOAT64) as expiryAllowanceMonths,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.terminalExpiryAllowanceMonths) AS FLOAT64) as terminalExpiryAllowanceMonths,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.discountRate) AS FLOAT64) as discountRate,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.capRateTerminal) AS FLOAT64) as capRateTerminal,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.cashFlowPeriod) AS INT64) as cashFlowPeriod,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.budgetedCapexAllowanceMonths) AS FLOAT64) as budgetedCapexAllowanceMonths,
      SAFE_CAST(JSON_VALUE(full_json.inputs.general.sinkingFundAllowanceMonths) AS FLOAT64) as sinkingFundAllowanceMonths,
      -- Added Futures Base Settings
      JSON_VALUE(full_json.inputs.futures.marketYearBasis) as futureMarketYearBasis,
      JSON_VALUE(full_json.inputs.futures.outgoingsYearBasis) as futureOutgoingsYearBasis,
      JSON_VALUE(full_json.inputs.futures.escalationYearBasis) as futureEscalationYearBasis,
      JSON_VALUE(full_json.inputs.futures.marketRentGrowthBasis) as futureMarketRentGrowthBasis,
      SAFE_CAST(JSON_VALUE(full_json.inputs.futures.priorYearCpi) AS FLOAT64) as futurePriorYearCpi,
      SAFE_CAST(JSON_VALUE(full_json.inputs.futures.deferOutgoings) AS BOOL) as futureDeferOutgoings;

    -- ==========================================
    -- SECTION 2: GRANULAR TENANT & SPACE DATA
    -- ==========================================

    -- TABLE 5: inputs_spaces_lease (100% Captured Space & Base Lease Data)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_spaces_lease\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(s.level) as level,
      JSON_VALUE(s.suite) as suite,
      JSON_VALUE(s.lease.tenantName) as tenantName,
      JSON_VALUE(s.rentBasis) as rentBasis,
      SAFE_CAST(JSON_VALUE(s.lettableArea) AS FLOAT64) as lettableArea,
      SAFE_CAST(JSON_VALUE(s.carBays) AS INT64) as carBays,
      JSON_VALUE(s.floorAreaType) as floorAreaType,
      JSON_VALUE(s.viewAspect) as viewAspect,
      JSON_VALUE(s.renewalAssumptionName) as renewalAssumptionName,
      JSON_VALUE(s.marketAssumption) as marketAssumption,
      SAFE_CAST(JSON_VALUE(s.lease.baseRent) AS FLOAT64) as baseRent,
      JSON_VALUE(s.lease.startDate) as startDate,
      JSON_VALUE(s.lease.expiryDate) as expiryDate,
      JSON_VALUE(s.lease.agreementType) as agreementType,
      JSON_VALUE(s.lease.industry) as industry,
      JSON_VALUE(s.lease.subIndustry) as subIndustry,
      JSON_VALUE(s.lease.classCode) as classCode,
      SAFE_CAST(JSON_VALUE(s.lease.isHoldOver) AS BOOL) as isHoldOver,
      SAFE_CAST(JSON_VALUE(s.lease.holdOverMonths) AS INT64) as holdOverMonths,
      JSON_VALUE(s.lease.breakDate) as breakDate,
      SAFE_CAST(JSON_VALUE(s.lease.breakPenalty) AS FLOAT64) as breakPenalty,
      JSON_VALUE(s.lease.breakNoticeDate) as breakNoticeDate,
      JSON_VALUE(s.lease.options) as options,
      SAFE_CAST(JSON_VALUE(s.lease.headOfAgreement) AS BOOL) as headOfAgreement,
      JSON_VALUE(s.lease.makeGood) as makeGood,
      JSON_VALUE(s.lease.headOfAgreementSignedDate) as headOfAgreementSignedDate,
      JSON_VALUE(s.lease.leaseSignedDate) as leaseSignedDate,
      SAFE_CAST(JSON_VALUE(s.lease.isConfidential) AS BOOL) as isConfidential,
      SAFE_CAST(JSON_VALUE(s.lease.isConfirmed) AS BOOL) as isConfirmed,
      JSON_VALUE(s.lease.informationSource) as informationSource,
      JSON_VALUE(s.lease.comments) as comments,
      JSON_VALUE(s.lease.priorReviewDate) as priorReviewDate,
      JSON_VALUE(s.lease.carLevy) as carLevy,
      SAFE_CAST(JSON_VALUE(s.lease.increaseOverBaseRecoveries) AS FLOAT64) as increaseOverBaseRecoveries,
      SAFE_CAST(JSON_VALUE(s.lease.semiGrossRecoveries) AS FLOAT64) as semiGrossRecoveries,
      JSON_VALUE(s.lease.netRecoveries.recoveryCode) as netRecoveryCode,
      SAFE_CAST(JSON_VALUE(s.lease.netRecoveries.amountPerAnnum) AS FLOAT64) as netRecoveryAmount,
      JSON_VALUE(s.lease.netRecoveries.type) as netRecoveryType
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.spaces)) AS s;

    -- TABLE 6: inputs_spaces_incentives (100% Captured Financial Abatements)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_spaces_incentives\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(s.lease.tenantName) as tenantName,
      JSON_VALUE(s.level) as level,
      JSON_VALUE(s.suite) as suite,
      SAFE_CAST(JSON_VALUE(s.lease.outstandingIncentives.lumpSumIncentives.amount) AS FLOAT64) as outLumpSumAmount,
      JSON_VALUE(s.lease.outstandingIncentives.lumpSumIncentives.paymentDate) as outLumpSumPaymentDate,
      SAFE_CAST(JSON_VALUE(s.lease.outstandingIncentives.rebateIncentives.amountMonthly) AS FLOAT64) as outRebateAmountMonthly,
      JSON_VALUE(s.lease.outstandingIncentives.rebateIncentives.startDate) as outRebateStartDate,
      JSON_VALUE(s.lease.outstandingIncentives.rebateIncentives.endDate) as outRebateEndDate,
      JSON_VALUE(s.lease.outstandingIncentives.rentFreeIncentives.basis) as outRentFreeBasis,
      SAFE_CAST(JSON_VALUE(s.lease.outstandingIncentives.rentFreeIncentives.percentApplied) AS FLOAT64) as outRentFreePercent,
      JSON_VALUE(s.lease.outstandingIncentives.rentFreeIncentives.startDate) as outRentFreeStartDate,
      JSON_VALUE(s.lease.outstandingIncentives.rentFreeIncentives.endDate) as outRentFreeEndDate,
      SAFE_CAST(JSON_VALUE(s.lease.commencementIncentives.commencementBaseRent) AS FLOAT64) as comBaseRent,
      SAFE_CAST(JSON_VALUE(s.lease.commencementIncentives.commencementRecoveries) AS FLOAT64) as comRecoveries,
      SAFE_CAST(JSON_VALUE(s.lease.commencementIncentives.total) AS FLOAT64) as comTotal,
      JSON_VALUE(s.lease.commencementIncentives.type) as comType,
      SAFE_CAST(JSON_VALUE(s.lease.commencementIncentives.incentiveAmortisation.paidToDate) AS FLOAT64) as comAmortPaidToDate,
      JSON_VALUE(s.lease.commencementIncentives.incentiveAmortisation.startDate) as comAmortStartDate
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.spaces)) AS s
    WHERE JSON_VALUE(s.lease.tenantName) IS NOT NULL AND JSON_VALUE(s.lease.tenantName) != '-';

    -- TABLE 7: inputs_spaces_first_renewal (100% Captured Local Renewals)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_spaces_first_renewal\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(s.lease.tenantName) as tenantName,
      JSON_VALUE(s.level) as level,
      JSON_VALUE(s.suite) as suite,
      JSON_VALUE(s.firstRenewal.type) as renewalType,
      JSON_VALUE(s.firstRenewal.tenantName) as renewalTenantName,
      JSON_VALUE(s.firstRenewal.startDate) as renewalStartDate,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.leaseTermYears) AS FLOAT64) as leaseTermYears,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.useInWale) AS BOOL) as useInWale,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.renewalProbabilityPercent) AS FLOAT64) as renewalProbabilityPercent,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.refurbPerMetreSquared) AS FLOAT64) as refurbPerMetreSquared,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.downtimeMonths) AS FLOAT64) as downtimeMonths,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.incentiveLumpSumPercent) AS FLOAT64) as incentiveLumpSumPercent,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.incentiveRentFreePercent) AS FLOAT64) as incentiveRentFreePercent,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.incentiveRebatePercent) AS FLOAT64) as incentiveRebatePercent,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.appliedIncentivesPercent) AS FLOAT64) as appliedIncentivesPercent,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.leasingCostsPercent) AS FLOAT64) as leasingCostsPercent,
      JSON_VALUE(s.firstRenewal.commencementRentReview.type) as reviewType,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.commencementRentReview.cap) AS FLOAT64) as reviewCap,
      SAFE_CAST(JSON_VALUE(s.firstRenewal.commencementRentReview.collar) AS FLOAT64) as reviewCollar
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.spaces)) AS s
    WHERE JSON_VALUE(s.firstRenewal.renewalProbabilityPercent) IS NOT NULL;

    -- ==========================================
    -- SECTION 3: OUTGOINGS, CAPEX & ASSUMPTIONS
    -- ==========================================

    -- TABLE 8: inputs_outgoings (100% Captured)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_outgoings\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(full_json.inputs.outgoings.outgoingsDate) as outgoingsDate,
      JSON_VALUE(item.name) as name,
      JSON_VALUE(item.type) as type,
      JSON_VALUE(item.pcaCategory) as pcaCategory,
      SAFE_CAST(JSON_VALUE(item.isRecoverable) AS BOOL) as isRecoverable,
      JSON_VALUE(item.growthRateName) as growthRateName,
      SAFE_CAST(JSON_VALUE(item.previousBudget) AS FLOAT64) as previousBudget,
      SAFE_CAST(JSON_VALUE(item.currentBudget) AS FLOAT64) as currentBudget,
      SAFE_CAST(JSON_VALUE(item.adoptedBudget) AS FLOAT64) as adoptedBudget
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.outgoings.items)) AS item;

    -- TABLE 9: inputs_capex_budget (Header Data 100% Captured)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_capex_budget\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(capex.name) as name,
      JSON_VALUE(capex.category) as category,
      JSON_VALUE(capex.type) as type,
      JSON_VALUE(capex.growthRateName) as growthRateName,
      JSON_VALUE(capex.startDate) as startDate,
      SAFE_CAST(JSON_VALUE(capex.months) AS INT64) as months,
      SAFE_CAST(JSON_VALUE(capex.totalAmount) AS FLOAT64) as totalAmount
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.capex.budgetedCapexs)) AS capex;

    -- TABLE 10: inputs_capex_forecasts (Unrolled Array)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_capex_forecasts\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(capex.name) as name,
      year_offset as year_index,
      SAFE_CAST(JSON_VALUE(amount) AS FLOAT64) as forecastAmount
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.capex.budgetedCapexs)) AS capex,
         UNNEST(JSON_QUERY_ARRAY(capex.amounts)) AS amount WITH OFFSET as year_offset;

    -- TABLE 11: inputs_escalations (Unrolled Array)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_escalations\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(esc.name) as categoryName,
      SAFE_CAST(JSON_VALUE(esc.increaseOnCpi) AS FLOAT64) as cpiPremium,
      year_offset as year_index,
      SAFE_CAST(JSON_VALUE(rate) AS FLOAT64) as escalationRate
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.futures.escalationAssumptions)) AS esc,
         UNNEST(JSON_QUERY_ARRAY(esc.rates)) AS rate WITH OFFSET as year_offset;

    -- TABLE 12: inputs_market_growth (Unrolled Array)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_market_growth\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(mkt.name) as categoryName,
      SAFE_CAST(JSON_VALUE(mkt.increaseOnCpi) AS FLOAT64) as cpiPremium,
      year_offset as year_index,
      SAFE_CAST(JSON_VALUE(rate) AS FLOAT64) as marketGrowthRate
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.futures.marketGrowthAssumptions)) AS mkt,
         UNNEST(JSON_QUERY_ARRAY(mkt.rates)) AS rate WITH OFFSET as year_offset;

    -- TABLE 13: inputs_renewal_assumptions (Market Rules 100% Captured)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_renewal_assumptions\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(ren.name) as name,
      SAFE_CAST(JSON_VALUE(ren.vacantDowntimeMonths) AS FLOAT64) as vacantDowntimeMonths,
      SAFE_CAST(JSON_VALUE(ren.renewalProbabilityPercent) AS FLOAT64) as renewalProbabilityPercent,
      SAFE_CAST(JSON_VALUE(ren.newLeaseYears) AS INT64) as newLeaseYears,
      SAFE_CAST(JSON_VALUE(ren.newLeaseReviewYears) AS INT64) as newLeaseReviewYears,
      JSON_VALUE(ren.newLeaseStructure) as newLeaseStructure,
      SAFE_CAST(JSON_VALUE(ren.futureDowntimeMonths) AS FLOAT64) as futureDowntimeMonths,
      SAFE_CAST(JSON_VALUE(ren.incentivesPercent) AS FLOAT64) as incentivesPercent,
      SAFE_CAST(JSON_VALUE(ren.incentivesProbabilityPercent) AS FLOAT64) as incentivesProbabilityPercent,
      SAFE_CAST(JSON_VALUE(ren.leasingCostsRenewingPercent) AS FLOAT64) as leasingCostsRenewingPercent,
      SAFE_CAST(JSON_VALUE(ren.leasingCostsNewPercent) AS FLOAT64) as leasingCostsNewPercent,
      SAFE_CAST(JSON_VALUE(ren.refurbAllowanceRenewing) AS FLOAT64) as refurbAllowanceRenewing,
      SAFE_CAST(JSON_VALUE(ren.refurbAllowanceNew) AS FLOAT64) as refurbAllowanceNew,
      SAFE_CAST(JSON_VALUE(ren.marketCapRate) AS FLOAT64) as marketCapRate
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.futures.renewalAssumptions)) AS ren;

    -- TABLE 14: inputs_renewal_forecasts (Unrolled Dual Arrays via Offset Join)
    CREATE OR REPLACE TABLE \`${targetDataset}.inputs_renewal_forecasts\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(ren.name) as categoryName,
      offset_a as year_index,
      SAFE_CAST(JSON_VALUE(inc) AS FLOAT64) as newLeaseIncentivePercent,
      SAFE_CAST(JSON_VALUE(prob) AS FLOAT64) as incentiveProbabilityPercent
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.inputs.futures.renewalAssumptions)) AS ren
    LEFT JOIN UNNEST(JSON_QUERY_ARRAY(ren.newLeaseIncentives)) AS inc WITH OFFSET as offset_a
    LEFT JOIN UNNEST(JSON_QUERY_ARRAY(ren.incentiveProbabilityPercentages)) AS prob WITH OFFSET as offset_b
      ON offset_a = offset_b;

    -- ==========================================
    -- SECTION 4: OUTPUTS & VALUATION METRICS
    -- ==========================================

    -- TABLE 15: outputs_master_metrics (Massive Wide Table of all Scalar Outputs)
    CREATE OR REPLACE TABLE \`${targetDataset}.outputs_master_metrics\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      -- Key Metrics
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.roundedAdoptedValuation) AS FLOAT64) as roundedAdoptedValuation,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.additionalLandValue) AS FLOAT64) as additionalLandValue,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.initialPassingYield) AS FLOAT64) as initialPassingYield,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.initialPassingYieldFullyLeased) AS FLOAT64) as initialPassingYieldFullyLeased,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.equivalentMarketYield) AS FLOAT64) as equivalentMarketYield,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.equivalentInitialYield) AS FLOAT64) as equivalentInitialYield,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.equivalentReversionaryYield) AS FLOAT64) as equivalentReversionaryYield,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.terminalYield) AS FLOAT64) as terminalYield,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.tenYearIRR) AS FLOAT64) as tenYearIRR,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.capitalValuePsqm) AS FLOAT64) as capitalValuePsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.capitalValueSiteAreaPsqm) AS FLOAT64) as capitalValueSiteAreaPsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.keyMetrics.capitalValueAdditionalLandPsqm) AS FLOAT64) as capitalValueAdditionalLandPsqm,
      -- Income Analysis Scalars
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.grossPassingIncomePA) AS FLOAT64) as grossPassingIncomePA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.grossPassingIncomePsqm) AS FLOAT64) as grossPassingIncomePsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.grossMarketIncomePA) AS FLOAT64) as grossMarketIncomePA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.grossMarketIncomePsqm) AS FLOAT64) as grossMarketIncomePsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.netPassingIncomePA) AS FLOAT64) as netPassingIncomePA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.netPassingIncomePsqm) AS FLOAT64) as netPassingIncomePsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.netMarketIncomePA) AS FLOAT64) as netMarketIncomePA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.netMarketIncomePsqm) AS FLOAT64) as netMarketIncomePsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.netPassingIncomeFullyLeasedPA) AS FLOAT64) as netPassingIncomeFullyLeasedPA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.outgoingsPA) AS FLOAT64) as totalOutgoingsPA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.outgoingsPsqm) AS FLOAT64) as totalOutgoingsPsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.statutoryOutgoingsPsqm) AS FLOAT64) as statutoryOutgoingsPsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.operatingOutgoingsPsqm) AS FLOAT64) as operatingOutgoingsPsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.nonRecoverableOutgoingsPsqm) AS FLOAT64) as nonRecoverableOutgoingsPsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.specialIncome) AS FLOAT64) as incSpecialIncome,
      SAFE_CAST(JSON_VALUE(full_json.outputs.incomeAnalysis.carparkingGrossIncomePassingPA) AS FLOAT64) as carparkingGrossIncomePassingPA,
      -- Occupancy Area Scalars
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.siteArea) AS FLOAT64) as outSiteArea,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.lettableArea) AS FLOAT64) as outLettableArea,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.occupiedArea) AS FLOAT64) as occupiedArea,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.occupiedPercent) AS FLOAT64) as occupiedPercent,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.vacantArea) AS FLOAT64) as vacantArea,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.vacantAreaPercent) AS FLOAT64) as vacantAreaPercent,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.vacantIncomePercent) AS FLOAT64) as vacantIncomePercent,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.vacantTenantsCount) AS INT64) as vacantTenantsCount,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.tenantsCount) AS INT64) as tenantsCount,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.carBays) AS FLOAT64) as outCarBays,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.carParkRatio) AS FLOAT64) as carParkRatio,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.lettableAreaPerCarPark) AS FLOAT64) as lettableAreaPerCarPark,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.waleByIncome) AS FLOAT64) as waleByIncome,
      SAFE_CAST(JSON_VALUE(full_json.outputs.occupancyArea.waleByArea) AS FLOAT64) as waleByArea,
      -- Growth & Capex Assumptions
      SAFE_CAST(JSON_VALUE(full_json.outputs.growthAssumptions.grossMarketIncomeTenYearCAGR) AS FLOAT64) as grossMarketIncomeTenYearCAGR,
      SAFE_CAST(JSON_VALUE(full_json.outputs.growthAssumptions.outgoingsTenYearCAGR) AS FLOAT64) as outgoingsTenYearCAGR,
      SAFE_CAST(JSON_VALUE(full_json.outputs.growthAssumptions.cpiTenYearCAGR) AS FLOAT64) as cpiTenYearCAGR,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capexAssumptions.tenYearCapex) AS FLOAT64) as tenYearCapex,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capexAssumptions.tenYearCapexPsqm) AS FLOAT64) as tenYearCapexPsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capexAssumptions.tenYearCapexPresentValue) AS FLOAT64) as tenYearCapexPresentValue,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capexAssumptions.tenYearCapexTerminalInclusive) AS FLOAT64) as tenYearCapexTerminalInclusive,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capexAssumptions.tenYearCapexTerminalInclusivePsqm) AS FLOAT64) as tenYearCapexTerminalInclusivePsqm,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capexAssumptions.tenYearCapexTerminalInclusivePresentValue) AS FLOAT64) as tenYearCapexTerminalInclusivePresentValue;

    -- TABLE 16: outputs_valuation_dcf (Deep Dive into Core Math)
    CREATE OR REPLACE TABLE \`${targetDataset}.outputs_valuation_dcf\` AS
    SELECT 
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      -- Capitalisation Method
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.roundedCapitalisedValue) AS FLOAT64) as roundedCapitalisedValue,
      JSON_VALUE(full_json.outputs.capitalisation.expiryAllowanceDate) as capExpiryAllowanceDate,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.expiryAllowanceNetLettableAreaPercent) AS FLOAT64) as capExpiryAllowanceNLA_Pct,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.sundryIncomePA) AS FLOAT64) as capSundryIncomePA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.vacancyAllowancePA) AS FLOAT64) as capVacancyAllowancePA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.netMarketIncomePA) AS FLOAT64) as capNetMarketIncomePA,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalisedAdjustments) AS FLOAT64) as capitalisedAdjustments,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalisationRate) AS FLOAT64) as capitalisationRate,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalisedCoreValue) AS FLOAT64) as capitalisedCoreValue,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalisedValue) AS FLOAT64) as capitalisedValue,
      -- Cap Value Adjustments (Nested in Capitalisation)
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalValues.rentalReversions) AS FLOAT64) as capAdj_RentalReversions,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalValues.specialIncome) AS FLOAT64) as capAdj_SpecialIncome,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalValues.stabilisation) AS FLOAT64) as capAdj_Stabilisation,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalValues.otherAdjustments) AS FLOAT64) as capAdj_Other,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalValues.outstandingIncentives) AS FLOAT64) as capAdj_OutstandingIncentives,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalValues.additionalLand) AS FLOAT64) as capAdj_AdditionalLand,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalValues.budgetedCapex) AS FLOAT64) as capAdj_BudgetedCapex,
      SAFE_CAST(JSON_VALUE(full_json.outputs.capitalisation.capitalValues.sinkingFund) AS FLOAT64) as capAdj_SinkingFund,
      -- DCF Master
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.discountRate) AS FLOAT64) as dcfDiscountRate,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.sinkingFundHorizon) AS FLOAT64) as dcfSinkingFundHorizon,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.waleByArea) AS FLOAT64) as dcfWaleByArea,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.waleByRent) AS FLOAT64) as dcfWaleByRent,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.acquisitionCostsPercent) AS FLOAT64) as dcfAcquisitionCostsPercent,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.disposalCostsPercent) AS FLOAT64) as dcfDisposalCostsPercent,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.sumDiscountedCashflows) AS FLOAT64) as sumDiscountedCashflows,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.sumDiscountedCashflowsPercent) AS FLOAT64) as sumDiscountedCashflowsPercent,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.terminalValuePresentValue) AS FLOAT64) as terminalValuePresentValue,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.terminalValuePresentValuePercent) AS FLOAT64) as terminalValuePresentValuePercent,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.netPresentValueBeforeAcquisition) AS FLOAT64) as netPresentValueBeforeAcquisition,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.acquisitionCosts) AS FLOAT64) as acquisitionCosts,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.netPresentValue) AS FLOAT64) as netPresentValue,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.roundedNetPresentValue) AS FLOAT64) as roundedNetPresentValue,
      -- DCF Hold Period Aggregates
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.percentageRent) AS FLOAT64) as holdPercentageRent,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.recoveries) AS FLOAT64) as holdRecoveries,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.sundryIncome) AS FLOAT64) as holdSundryIncome,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.specialIncome) AS FLOAT64) as holdSpecialIncome,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.statutoryExpenses) AS FLOAT64) as holdStatutoryExpenses,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.operatingExpenses) AS FLOAT64) as holdOperatingExpenses,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.groundRent) AS FLOAT64) as holdGroundRent,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.nonRecoverableExpenses) AS FLOAT64) as holdNonRecoverableExpenses,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.vacancyAllowance) AS FLOAT64) as holdVacancyAllowance,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.incentives) AS FLOAT64) as holdIncentives,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.leasingCosts) AS FLOAT64) as holdLeasingCosts,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.refurbishment) AS FLOAT64) as holdRefurbishment,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.budgetedCapex) AS FLOAT64) as holdBudgetedCapex,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.sinkingFund) AS FLOAT64) as holdSinkingFund,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.holdPeriodCashflow.totalCapitalExpenditure) AS FLOAT64) as holdTotalCapitalExpenditure,
      -- Terminal Valuation
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.terminalValuation.disposalCosts) AS FLOAT64) as termDisposalCosts,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.terminalValuation.netSaleProceeds) AS FLOAT64) as termNetSaleProceeds,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.terminalValuation.capitalisedAdjustments) AS FLOAT64) as termCapitalisedAdjustments,
      SAFE_CAST(JSON_VALUE(full_json.outputs.discountedCashflow.terminalValuation.capitalisedValue) AS FLOAT64) as termCapitalisedValue;

    -- TABLE 17: outputs_categories (Join multiple category arrays into one row per space-type)
    CREATE OR REPLACE TABLE \`${targetDataset}.outputs_categories\` AS
    SELECT
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(cat.name) as categoryName,
      SAFE_CAST(JSON_VALUE(cat.rank) AS INT64) as rank,
      -- Area
      SAFE_CAST(JSON_VALUE(area.area) AS FLOAT64) as area,
      SAFE_CAST(JSON_VALUE(area.tenantsCount) AS INT64) as tenantsCount,
      -- Incomes
      SAFE_CAST(JSON_VALUE(cat.averageGrossPassingPsqm) AS FLOAT64) as averageGrossPassingPsqm,
      SAFE_CAST(JSON_VALUE(cat.averageGrossMarketPsqm) AS FLOAT64) as averageGrossMarketPsqm,
      SAFE_CAST(JSON_VALUE(cat.averageNetPassingPsqm) AS FLOAT64) as averageNetPassingPsqm,
      SAFE_CAST(JSON_VALUE(cat.averageNetMarketPsqm) AS FLOAT64) as averageNetMarketPsqm,
      -- Growths
      SAFE_CAST(JSON_VALUE(grw.grossMarketFaceTenYearCAGR) AS FLOAT64) as grossMarketFaceTenYearCAGR,
      SAFE_CAST(JSON_VALUE(grw.netMarketFaceTenYearCAGR) AS FLOAT64) as netMarketFaceTenYearCAGR,
      SAFE_CAST(JSON_VALUE(grw.grossMarketEffectiveTenYearCAGR) AS FLOAT64) as grossMarketEffectiveTenYearCAGR,
      SAFE_CAST(JSON_VALUE(grw.netMarketEffectiveTenYearCAGR) AS FLOAT64) as netMarketEffectiveTenYearCAGR
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.outputs.incomeAnalysis.renewalTypeIncomes)) AS cat
    LEFT JOIN UNNEST(JSON_QUERY_ARRAY(full_json.outputs.occupancyArea.renewalTypeAreas)) AS area 
           ON JSON_VALUE(cat.name) = JSON_VALUE(area.name)
    LEFT JOIN UNNEST(JSON_QUERY_ARRAY(full_json.outputs.growthAssumptions.renewalTypeGrowths)) AS grw 
           ON JSON_VALUE(cat.name) = JSON_VALUE(grw.name);

    -- TABLE 18: outputs_dcf_categories (DCF Specific Categorical Data Joined)
    CREATE OR REPLACE TABLE \`${targetDataset}.outputs_dcf_categories\` AS
    SELECT
      JSON_VALUE(full_json.attributes.modelId) as modelId,
      JSON_VALUE(cap.name) as categoryName,
      SAFE_CAST(JSON_VALUE(cap.incomePA) AS FLOAT64) as capGrossMarketIncomePA,
      SAFE_CAST(JSON_VALUE(term.incomePA) AS FLOAT64) as terminalGrossMarketIncomePA,
      SAFE_CAST(JSON_VALUE(hp.rent) AS FLOAT64) as holdPeriodBaseRent
    FROM UNNEST(JSON_QUERY_ARRAY(full_json.outputs.capitalisation.renewalTypeGrossMarketIncomes)) AS cap
    LEFT JOIN UNNEST(JSON_QUERY_ARRAY(full_json.outputs.discountedCashflow.terminalValuation.renewalTypeGrossMarketIncomes)) AS term 
           ON JSON_VALUE(cap.name) = JSON_VALUE(term.name)
    LEFT JOIN UNNEST(JSON_QUERY_ARRAY(full_json.outputs.discountedCashflow.holdPeriodCashflow.renewalTypeBaseRents)) AS hp 
           ON JSON_VALUE(cap.name) = JSON_VALUE(hp.name);

  END;
`);