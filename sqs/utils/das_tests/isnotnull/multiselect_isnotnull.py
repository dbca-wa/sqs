GEOJSON = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[124.12353515624999, -30.391830328088137], [124.03564453125, -31.672083485607377], [126.69433593749999, -31.615965936476076], [127.17773437499999, -29.688052749856787], [124.12353515624999, -30.391830328088137]]]}}]}


PROPOSAL = {
  "id": 1522,
  "schema": [
    {
      "name": "proposalSelectSection",
      "type": "section",
      "label": "2. Proposal Select",
      "children": [
        {
          "name": "Section2-0",
          "type": "multi-select",
          "label": "2.0 In which something is this proposal located (Multi-select Component)?",
          "options": [
            {
              "label": "SHIRE OF GOOMALLING",
              "value": "SHIRE-OF-GOOMALLING",
              "isRequired": "true"
            },
            {
              "label": "SHIRE OF PERENJORI",
              "value": "SHIRE-OF-PERENJORI"
            },
            {
              "label": "SHIRE OF DUNDAS",
              "value": "SHIRE-OF-DUNDAS"
            },
            {
              "label": "SHIRE OF YORK",
              "value": "SHIRE-OF-YORK"
            },
            {
              "label": "SHIRE OF NORTHAM",
              "value": "SHIRE-OF-NORTHAM"
            },
            {
              "label": "CITY OF KALGOORLIE-BOULDER",
              "value": "CITY-OF-KALGOORLIE-BOULDER"
            },
            {
              "label": "CITY OF JOONDALUP",
              "value": "CITY-OF-JOONDALUP"
            }
          ],
          "help_text_url": "site_url:/help/disturbance/user/anchor=Section2-2"
        }
      ]
    }
  ],
  "data": []
}


MASTERLIST_QUESTIONS_GBQ = [
  {
    "question_group": "2.0 In which something is this proposal located (Multi-select Component)?",
    "questions": [
      {
        "id": 44,
        "question": "2.0 In which something is this proposal located (Multi-select Component)?",
        "answer_mlq": "SHIRE OF DUNDAS",
        "layer_name": "cddp:local_gov_authority",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:local_gov_authority&maxFeatures=200&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "lga_label",
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
] 


TEST_RESPONSE = {
  "system": "DAS",
  "data": [
    {
      "proposalSelectSection": [
        {
          "Section2-0": [
            "SHIRE-OF-DUNDAS"
          ]
        }
      ]
    }
  ],
  "layer_data": [
    {
      "name": "Section2-0",
      "label": None,
      "layer_name": "cddp:local_gov_authority",
      "layer_created": "2022-05-17 07:28:48",
      "layer_version": 1,
      "sqs_timestamp": "2023-03-10 17:13:56"
    }
  ],
  "add_info_assessor": {}
}


