'''
das_query = requests.get('http://localhost:8003/api/proposal/1525/sqs_data.json').json()
dlq = DisturbanceLayerQuery(das_query['masterlist_questions'], das_query['geojson'], das_query['proposal'])

MASTERLIST_QUESTIONS_GBQ = json.dumps(das_query['masterlist_questions'])
PROPOSAL                 = json.dumps(das_query['proposal'])
TEST_RESPONSE            = json.dumps(dlq.query())

'''


GEOJSON = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[124.12353515624999, -30.391830328088137], [124.03564453125, -31.672083485607377], [126.69433593749999, -31.615965936476076], [127.17773437499999, -29.688052749856787], [124.12353515624999, -30.391830328088137]]]}}]}


PROPOSAL = {
  "id": 1525,
  "schema": [
    {
      "name": "proposalSummarySection",
      "type": "section",
      "label": "7. Proposal Summary",
      "children": [
        {
          "name": "Section7-0",
          "type": "text",
          "label": "7.0 Proposal title (Text Component)?",
          "isRequired": "true",
          "help_text_url": "site_url:/help/disturbance/user/anchor=Section7-0",
          "_help_text_assessor_url": "site_url:/help/disturbance/assessor/anchor=Section7-0",
          "isTitleColumnForDashboard": True
        },
        {
          "name": "Section8-0",
          "type": "text_area",
          "label": "8.0 Proposal subtitle (Textbox Component)?",
          "isRequired": "true",
          "help_text_url": "site_url:/help/disturbance/user/anchor=Section8-0",
          "_help_text_assessor_url": "site_url:/help/disturbance/assessor/anchor=Section8-0",
          "isTitleColumnForDashboard": True
        },
        {
          "name": "Section9-0",
          "type": "text_area",
          "label": "9.0 Proposal something else ... (Textbox Component)?",
          "isRequired": "true",
          "help_text_url": "site_url:/help/disturbance/user/anchor=Section9-0",
          "_help_text_assessor_url": "site_url:/help/disturbance/assessor/anchor=Section9-0",
          "isTitleColumnForDashboard": True
        }

      ]
    }
  ],
  "data": [
  ]
}


MASTERLIST_QUESTIONS_GBQ = [
  {
    "question_group": "8.0 Proposal subtitle (Textbox Component)?",
    "questions": [
      {
        "id": 54,
        "question": "8.0 Proposal subtitle (Textbox Component)?",
        "answer_mlq": "",
        "layer_name": "cddp:dpaw_regions",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "hectares",
        "operator": "LessThan",
        "value": "250000000000",
        "prefix_answer": "(ProponentPrefix)",
        "no_polygons_proponent": -1,
        "answer": "::region",
        "prefix_info": "(AssessorPrefix)",
        "no_polygons_assessor": -1,
        "assessor_info": "::region",
        "regions": "All"
      }
    ]
  },
  {
    "question_group": "7.0 Proposal title (Text Component)?",
    "questions": [
      {
        "id": 53,
        "question": "7.0 Proposal title (Text Component)?",
        "answer_mlq": "",
        "layer_name": "cddp:local_gov_authority",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:local_gov_authority&maxFeatures=200&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "lga_leg_ar",
        "operator": "LessThan",
        "value": "500000000000",
        "prefix_answer": "(ProponentPrefix)",
        "no_polygons_proponent": 1,
        "answer": "::lga_label",
        "prefix_info": "(AssessorPrefix)",
        "no_polygons_assessor": 1,
        "assessor_info": "::lga_label",
        "regions": "All"
      }
    ]
  }
]


TEST_RESPONSE = {
  "system": "DAS",
  "data": [
    {
      "proposalSummarySection": [
        {
          "Section7-0": "(ProponentPrefix) SHIRE OF DUNDAS",
          "Section8-0": "(ProponentPrefix) GOLDFIELDS, SOUTH COAST"
        }
      ]
    }
  ],
  "layer_data": [
    {
      "name": "Section7-0",
      "label": None,
      "layer_name": "cddp:local_gov_authority",
      "layer_created": "2022-05-17 07:28:48",
      "layer_version": 1,
      "layer_attrs": [
        {
          "ogc_fid": 15,
          "lga_date": None,
          "lga_type": "SHIRE",
          "lga_label": "SHIRE OF MENZIES",
          "lga_name1": "MENZIES, SHIRE OF",
          "lga_name2": "MENZIES",
          "lga_abs_nu": 5390,
          "lga_leg_ar": 124446356878
        },
        {
          "ogc_fid": 16,
          "lga_date": None,
          "lga_type": "CITY",
          "lga_label": "CITY OF KALGOORLIE-BOULDER",
          "lga_name1": "KALGOORLIE-BOULDER, CITY OF",
          "lga_name2": "KALGOORLIE-BOULDER",
          "lga_abs_nu": 4280,
          "lga_leg_ar": 95522776881.4
        },
        {
          "ogc_fid": 46,
          "lga_date": None,
          "lga_type": "SHIRE",
          "lga_label": "SHIRE OF DUNDAS",
          "lga_name1": "DUNDAS, SHIRE OF",
          "lga_name2": "DUNDAS",
          "lga_abs_nu": 3080,
          "lga_leg_ar": 95929937848.5
        }
      ],
      "sqs_timestamp": "2023-03-15 14:16:57"
    },
    {
      "name": "Section8-0",
      "label": None,
      "layer_name": "cddp:dpaw_regions",
      "layer_created": "2022-05-17 07:26:41",
      "layer_version": 1,
      "layer_attrs": [
        {
          "office": "KALGOORLIE",
          "region": "GOLDFIELDS",
          "ogc_fid": 0,
          "hectares": 84251815.4161
        },
        {
          "office": "ALBANY",
          "region": "SOUTH COAST",
          "ogc_fid": 4,
          "hectares": 18043736.7152
        }
      ],
      "sqs_timestamp": "2023-03-15 14:16:59"
    }
  ],
  "add_info_assessor": {
    "Section7-0": "(AssessorPrefix) SHIRE OF MENZIES",
    "Section8-0": "(AssessorPrefix) GOLDFIELDS, SOUTH COAST"
  }
}

