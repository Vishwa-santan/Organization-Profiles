profile = """
CALL apoc.load.json($file_path) YIELD value

// ------------------ ROOT: Business Profile ------------------
WITH value
MERGE (bp:BusinessProfile)
SET bp.created_at = datetime()

WITH bp, value, value.business_profile AS p

// ------------------ 1. Company ------------------
CREATE (c:Company {name: p.company_name})
SET c.insight = p.insight
MERGE (bp)-[:HAS_COMPANY]->(c)

// ------------------ 2. SIC Codes ------------------
WITH bp, c, p
UNWIND coalesce(p.sic_code, []) AS sc
MERGE (s:SICCode {code: sc.code})
SET s.description = sc.description
MERGE (bp)-[:HAS_SIC]->(s)
MERGE (c)-[:COMPANY_SIC]->(s)

// ------------------ 3. NAICS Codes ------------------
WITH bp, c, p
UNWIND coalesce(p.naics_codes, []) AS nc
MERGE (n:NAICSCode {code: nc.code})
SET n.description = nc.description
MERGE (bp)-[:HAS_NAICS]->(n)
MERGE (c)-[:COMPANY_NAICS]->(n)

// ------------------ 4. Primary Customer Segments ------------------
WITH bp, c, p
UNWIND coalesce(p.primary_customer_segments, []) AS seg
MERGE (pcs:PrimaryCustomerSegment {segment: seg.segment})
SET pcs.description = seg.description,
    pcs.needs_driver = seg.needs_driver
MERGE (bp)-[:HAS_CUSTOMER_SEGMENT]->(pcs)
MERGE (c)-[:SERVES]->(pcs)

// ------------------ 5. Market Segments ------------------
WITH bp, c, p
UNWIND coalesce(p.market_segments, []) AS mseg
MERGE (ms:MarketSegment {name: mseg.name})
SET ms.description = mseg.description
MERGE (bp)-[:HAS_MARKET_SEGMENT]->(ms)
MERGE (c)-[:PARTICIPATES_IN]->(ms)

// ------------------ 6. Key Market Drivers ------------------
WITH bp, c, p
UNWIND coalesce(p.key_market_drivers, []) AS kmd
MERGE (k:KeyMarketDriver {driver: kmd.driver})
SET k.impact = kmd.impact
MERGE (bp)-[:HAS_MARKET_DRIVER]->(k)
MERGE (c)-[:INFLUENCED_BY]->(k)

// ------------------ 7. Value Proposition ------------------
WITH bp, c, p
UNWIND coalesce(p.value_proposition, []) AS vp
MERGE (v:ValueProposition {title: vp.title})
SET v.description = vp.description
MERGE (bp)-[:HAS_VALUE_PROPOSITION]->(v)
MERGE (c)-[:PROPOSED]->(v)

// ------------------ 8. Go-To-Market Strategy ------------------
WITH bp, c, p
UNWIND coalesce(p.go_to_market_strategy, []) AS gtm
MERGE (g:GoToMarketStrategy {channel: gtm.channel})
SET g.approach = gtm.approach,
    g.messaging = gtm.messaging
MERGE (bp)-[:HAS_GTM]->(g)
MERGE (c)-[:HAS_STRATEGY]->(g)

// ------------------ 9. Strategic Goals & Objectives ------------------
WITH bp, c, value
WITH bp, c, value, value.strategic_goals_and_objectives AS sgo
WHERE sgo IS NOT NULL
MERGE (sg:StrategicGoalsAndObjectives {
    insight: sgo.insight,
    summary: sgo.summary
})
MERGE (bp)-[:HAS_SGO]->(sg)
MERGE (c)-[:HAS_GOALS]->(sg)

// Alignment Analysis
WITH sg, sgo
WITH sg, sgo.alignment_analysis AS aln
WHERE aln IS NOT NULL
MERGE (a:AlignmentAnalysis {
    market_opportunities: aln.market_opportunities,
    risks: aln.risks
})
MERGE (sg)-[:ALIGNMENT_ANALYSIS]->(a)

// ------------------ 10. Forward-Looking Strategic Roadmap ------------------
WITH bp, c, value
WITH bp, c, value, value.forward_looking_strategic_roadmap AS fls
WHERE fls IS NOT NULL
MERGE (fl:StrategicRoadmap {
    insight: fls.insight,
    competitive_positioning: fls.competitive_positioning
})
MERGE (bp)-[:HAS_FLS]->(fl)
MERGE (c)-[:HAS_ROADMAP]->(fl)

// ------------------ 11. Market Opportunities & Threats ------------------
WITH fl, fls
WITH fl, fls.market_opportunities_and_threats AS mo
WHERE mo IS NOT NULL
MERGE (mot:MarketOpportunitiesAndThreats)
MERGE (fl)-[:HAS_MOT]->(mot)

// Opportunities
WITH mot, mo
UNWIND coalesce(mo.opportunities, []) AS op
MERGE (O:Opportunity {title: op.title})
SET O.description = op.description
MERGE (mot)-[:HAS_OPPORTUNITY]->(_]()


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

client.query(profile.replace('$file_path',"https://github.com/Vishwa-santan/Organization-Profiles/raw/refs/heads/main/Profile_Files/business.json"))
time.sleep(2)

logger.info("Graph structure loaded successfully.")

client.close()

