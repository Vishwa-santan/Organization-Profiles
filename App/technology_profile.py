technology_profile = """
CALL apoc.load.json('$file_path') YIELD value
// 1. Create the Company node and TechnologyLandscape
MERGE (c:Company {name: value.company_name})
WITH c, value

MERGE (tl:TechnologyLandscape)
MERGE (c)-[:HAS]->(tl)
WITH c, value, tl

// 1.1 Core Technology Domains
UNWIND value.technology_landscape.core_technology_domains AS ctd
MERGE (ct:CoreTechnologyDomain {name: ctd})
MERGE (tl)-[:HAS_DOMAIN]->(ct)
WITH c, value, tl

// 1.2 Primary Platforms and Vendors
UNWIND value.technology_landscape.primary_platforms_and_vendors AS ppv
MERGE (pv:PrimaryPlatformVendor {
 name: ppv.name, 
 category: ppv.category,
 details: ppv.details
})
MERGE (tl)-[:HAS_PLATFORM_VENDOR]->(pv)
WITH c, value, tl

// 1.3 Cloud Deployment Model
UNWIND value.technology_landscape.cloud_deployment_model AS cdm
MERGE (cdmNode:CloudDeploymentModel {description: cdm})
MERGE (tl)-[:DEPLOYS]->(cdmNode)
WITH c, value, tl

// 1.4 Core Business Systems
UNWIND value.technology_landscape.core_business_systems AS cbs
MERGE (cb:CoreBusinessSystem {
   name: cbs.name, 
   is_mission_critical: cbs.is_mission_critical, 
   description: cbs.description
})
MERGE (tl)-[:HAS_BUSINESS_SYSTEM]->(cb)
WITH c, value

// 1.5 Digital Ecosystem Integration (Directly linked to Company)
MERGE (dei:DigitalEcosystemIntegration {description: value.technology_landscape.digital_ecosystem_integration})
MERGE (c)-[:INTEGRATES]->(dei)
WITH c, value

// 2. Governance and Operating Model
MERGE (gom:GovernanceAndOperatingModel {
  it_operating_model: value.governance_and_operating_model.it_operating_model, 
  data_governance_and_privacy_compliance: value.governance_and_operating_model.data_governance_and_privacy_compliance
})
MERGE (c)-[:OPERATES]->(gom)
WITH c, value, gom

// 2.1 Leadership Structure
UNWIND value.governance_and_operating_model.leadership_structure AS ls
MERGE (lead:LeadershipStructure {
  role: ls.role, 
  reports_to: ls.reports_to
})
MERGE (gom)-[:HAS_STRUCTURE]->(lead)
WITH c, value, gom

// 2.2 Governance Frameworks
UNWIND value.governance_and_operating_model.governance_frameworks AS gf
MERGE (gframe:GovernanceFramework {description: gf})
MERGE (gom)-[:HAS_FRAMEWORK]->(gframe)
WITH c, value

// 3. Digital Transformation & AI Adoption
MERGE (dt:DigitalTransformationAndAIAdoption {
 transformation_maturity: value.digital_transformation_and_ai_adoption.transformation_maturity
})
MERGE (c)-[:TRANSFORMS]->(dt)
WITH c, value, dt

// 3.1 AI and ML Integration
UNWIND value.digital_transformation_and_ai_adoption.ai_and_ml_integration AS aiml
MERGE (ai:AiAndMlIntegration {description: aiml})
MERGE (dt)-[:INTEGRATES]->(ai)
WITH c, value, dt

// 3.2 Innovation Ecosystem
UNWIND value.digital_transformation_and_ai_adoption.innovation_ecosystem AS ine
MERGE (ie:InnovationEcosystem {description: ine})
MERGE (dt)-[:PARTICIPATES_IN]->(ie) // Changed MERGES to PARTICIPATES_IN
WITH c, value, dt

// 3.3 Emerging Technology Adoption
UNWIND value.digital_transformation_and_ai_adoption.emerging_technology_adoption AS eta
MERGE (et:EmergingTechnologyAdoption {description: eta})
MERGE (dt)-[:ADOPTS]->(et)
WITH c, value

// 4. Cybersecurity Posture
MERGE (cp:CybersecurityPosture {
 third_party_risk_management: value.cybersecurity_posture.third_party_risk_management,
 resilience_measures: value.cybersecurity_posture.resilience_measures
})
MERGE (c)-[:HAS_POSTURE]->(cp) // Changed ALIGNS to HAS_POSTURE
WITH c, value, cp

// 4.1 Framework Alignment
UNWIND value.cybersecurity_posture.framework_alignment AS fa
MERGE (faNode:FrameworkAlignment {description: fa})
MERGE (cp)-[:ALIGNS_WITH]->(faNode)
WITH c, value, cp

// 4.2 Incident and Breach History
UNWIND value.cybersecurity_posture.incident_and_breach_history AS ibh
MERGE (ib:IncidentAndBreachHistory {
 description: ibh.description, 
 date: ibh.date,
 status: ibh.remediation_status
})
MERGE (cp)-[:HAS_INCIDENT]->(ib)
WITH c, value

// 5. Technology Investment and Roadmap
MERGE (tir:TechnologyInvestmentAndRoadmap {})
MERGE (c)-[:INVESTS_IN]->(tir)
WITH c, value, tir

// 5.1 Recent IT Modernization
UNWIND value.technology_investment_and_roadmap.recent_it_modernization AS rim
MERGE (rimNode:RecentITModernization {description: rim})
MERGE (tir)-[:HAS_DEVELOPED]->(rimNode)
WITH c, value, tir

// 5.2 Strategic Partnerships
UNWIND value.technology_investment_and_roadmap.strategic_partnerships AS stp
MERGE (sp:StrategicPartnership {
 partner_name: stp.partner_name,
 focus_area: stp.focus_area
})
MERGE (tir)-[:HAS_PARTNER]->(sp)
WITH c, value, tir

// 5.3 AI Driven Transformations
UNWIND value.technology_investment_and_roadmap.ai_driven_transformations AS aid
MERGE (ait:AiDrivenTransformation {description: aid})
MERGE (tir)-[:INVESTED_IN]->(ait)
WITH c, value

// 6. Forward Looking Strategy
// 6.1 IT Modernization
UNWIND value.forward_looking_strategy.it_modernization AS im
MERGE (imo:ItModernizationStrategy {description: im}) // Renamed node to avoid conflict
MERGE (c)-[:STRATEGY_MODERNIZES]->(imo) // Renamed relationship
WITH c, value

// 6.2 Governance Enhancements
UNWIND value.forward_looking_strategy.governance_enhancements AS ge
MERGE (gen:GovernanceEnhancement {description: ge})
MERGE (c)-[:HAS_ENHANCEMENT]->(gen)
WITH c, value

// 6.3 Zero Trust Resilience
UNWIND value.forward_looking_strategy.zero_trust_resilience AS zt
MERGE (ztr:ZeroTrustResilience {description: zt})
MERGE (c)-[:ATTAINS_ZT]->(ztr)
WITH c, value

// 6.4 Workforce Enablement
UNWIND value.forward_looking_strategy.workforce_enablement AS we
MERGE (weNode:WorkforceEnablement {description: we})
MERGE (c)-[:ENABLES]->(weNode)


"""


import os
import time
import logging
from app import Neo4jConnect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = Neo4jConnect()

health = client.check_health()
if health is not True:
    print("Neo4j connection error:", health)
    os._exit(1)

logger.info("Loading graph structure...")

client.query(technology_profile.replace('$file_path',"https://github.com/Vishwa-santan/Organization-Profiles/raw/refs/heads/main/Profile_Files/technology.json"))
time.sleep(2)

logger.info("Graph structure loaded successfully.")

client.close()




