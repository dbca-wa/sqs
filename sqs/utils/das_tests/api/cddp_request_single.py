CDDP_REQUEST_SINGLE_JSON = {
  "proposal": {
    "system": "DAS",
    "id": 1523,
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
            "isRequired": "True",
            "help_text_url": "site_url:/help/disturbance/user/anchor=Section7-0",
            "_help_text_assessor_url": "site_url:/help/disturbance/assessor/anchor=Section7-0",
            "isTitleColumnForDashboard": True
          },
          {
            "name": "Section8-0",
            "type": "text",
            "label": "8.0 Proposal subtitle (Textbox Component)?",
            "isRequired": "True",
            "help_text_url": "site_url:/help/disturbance/user/anchor=Section8-0",
            "_help_text_assessor_url": "site_url:/help/disturbance/assessor/anchor=Section8-0",
            "isTitleColumnForDashboard": True
          }
        ]
      }
    ],
    "data": []
  },
  "masterlist_questions": [
    {
      "question_group": "7.0 Proposal title (Text Component)?",
      "questions": [
        {
          "id": 54,
          "question": "7.0 Proposal title (Text Component)?",
          "answer_mlq": "",
          "layer_name": "cddp:dpaw_regions",
          "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson",
          "expiry": "2023-01-01",
          "visible_to_proponent": True,
          "buffer": 300,
          "how": "Overlapping",
          "column_name": "region",
          "operator": "IsNotNull",
          "value": "",
          "prefix_answer": "(PrefixProponent):",
          "no_polygons_proponent": -1,
          "answer": "::region",
          "prefix_info": "",
          "no_polygons_assessor": -1,
          "assessor_info": "",
          "regions": "All"
        }
      ]
    },
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
          "column_name": "region",
          "operator": "IsNotNull",
          "value": "",
          "prefix_answer": "(PrefixProponent):",
          "no_polygons_proponent": -1,
          "answer": "::region",
          "prefix_info": "",
          "no_polygons_assessor": -1,
          "assessor_info": "",
          "regions": "All"
        }
      ]
    }
  ],
  "geojson": {
    "type": "FeatureCollection",
    "features": [
      {
        "type": "Feature",
        "properties": {},
        "geometry": {
          "type": "Polygon",
          "coordinates": [
            [
              [
                124.12353515624999,
                -30.391830328088137
              ],
              [
                124.03564453125,
                -31.672083485607377
              ],
              [
                126.69433593749999,
                -31.615965936476076
              ],
              [
                127.17773437499999,
                -29.688052749856787
              ],
              [
                124.12353515624999,
                -30.391830328088137
              ]
            ]
          ]
        }
      }
    ]
  }
}

TEST_RESPONSE = {
  "system": "DAS",
  "data": [
    {
      "proposalSummarySection": [
        {
          "Section8-0": "(PrefixProponent): GOLDFIELDS, SOUTH COAST"
        }
      ]
    }
  ],
  "layer_data": [
    {
      "name": "Section8-0",
      "label": None,
      "layer_name": "cddp:dpaw_regions",
      "layer_version": 1,
      "layer_created_date": "2023-03-30 08:26:58",
      "layer_modified_date": "2023-03-30 08:26:58",
      "sqs_timestamp": "2023-03-30 08:26:59",
      "attrs": [
        {
          "ogc_fid": 0,
          "region": "GOLDFIELDS",
          "office": "KALGOORLIE",
          "hectares": 84251815.4161
        },
        {
          "ogc_fid": 4,
          "region": "SOUTH COAST",
          "office": "ALBANY",
          "hectares": 18043736.7152
        }
      ],
      "error_msg": ""
    }
  ],
  "add_info_assessor": {
    "Section8-0": []
  }
}
