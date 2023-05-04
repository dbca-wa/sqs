CDDP_REQUEST_INCORRECT_LAYER_ATTRS_JSON = {
  "proposal": {
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
            "isRequired": "True",
            "help_text_url": "site_url:/help/disturbance/user/anchor=Section7-0",
            "_help_text_assessor_url": "site_url:/help/disturbance/assessor/anchor=Section7-0",
            "isTitleColumnForDashboard": True
          },
          {
            "name": "Section8-0",
            "type": "text_area",
            "label": "8.0 Proposal subtitle (Textbox Component)?",
            "isRequired": "True",
            "help_text_url": "site_url:/help/disturbance/user/anchor=Section8-0",
            "_help_text_assessor_url": "site_url:/help/disturbance/assessor/anchor=Section8-0",
            "isTitleColumnForDashboard": True
          }
        ]
      }
    ],
    "data": [
      {
        "proposalSummarySection": [
          {
            "Section7-0": "GOLDFIELDS, SOUTH COAST",
            "Section8-0": "GOLDFIELDS, SOUTH COAST"
          }
        ]
      }
    ]
  },
  "masterlist_questions": [
    {
      "question_group": "8.0 Proposal subtitle (Textbox Component)?",
      "questions": [
        {
          "id": 54,
          "question": "8.0 Proposal subtitle (Textbox Component)?",
          "answer_mlq": "",
          "layer_name": "public:dbca_trails_public",
          "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/public/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=public:dbca_trails_public&maxFeatures=50&outputFormat=application%2Fjson",
          "expiry": "2024-01-01",
          "visible_to_proponent": True,
          "buffer": 300,
          "how": "Overlapping",
          "column_name": "dummy_col",
          "operator": "IsNotNull",
          "value": "",
          "prefix_answer": "",
          "no_polygons_proponent": -1,
          "answer": "",
          "prefix_info": "",
          "no_polygons_assessor": -1,
          "assessor_info": "",
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
          "layer_name": "public:dbca_districts_public",
          "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/public/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=public:dbca_districts_public&maxFeatures=50&outputFormat=application%2Fjson",
          "expiry": "2024-01-01",
          "visible_to_proponent": True,
          "buffer": 300,
          "how": "Overlapping",
          "column_name": "dummy_col",
          "operator": "IsNotNull",
          "value": "",
          "prefix_answer": "",
          "no_polygons_proponent": -1,
          "answer": "",
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
