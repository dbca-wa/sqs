GEOJSON = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[124.12353515624999, -30.391830328088137], [124.03564453125, -31.672083485607377], [126.69433593749999, -31.615965936476076], [127.17773437499999, -29.688052749856787], [124.12353515624999, -30.391830328088137]]]}}]}


PROPOSAL = {
  "id": 1518,
  "schema": [
    {
      "name": "RadioTest",
      "type": "section",
      "label": "5. Radio Test",
      "children": [
        {
          "name": "Section5-0",
          "type": "radiobuttons",
          "label": "5.0 What is ... First level radiobutton (Radiobutton Component)?",
          "options": [
            {
              "label": "Yes",
              "value": "yes"
            },
            {
              "label": "No",
              "value": "no"
            },
            {
              "label": "Possibly",
              "value": "possibly"
            },
            {
              "label": "Possibly not",
              "value": "Possibly-not"
            },
            {
              "label": "Unknown",
              "value": "unknown"
            }
          ],
          "conditions": {
            "yes": [
              {
                "name": "Section5-0-YesGroup",
                "type": "group",
                "label": "",
                "children": [
                  {
                    "name": "Section5-0-Yes1",
                    "type": "radiobuttons",
                    "label": "6.0 What is ... Second Nested level radiobutton (Radiobutton Component)?",
                    "options": [
                      {
                        "label": "One option",
                        "value": "oneoption"
                      },
                      {
                        "label": "two option",
                        "value": "twooption"
                      }
                    ]
                  }
                ]
              }
            ]
          }
        }
      ]
    }
  ],
  "data": []
}


MASTERLIST_QUESTIONS_GBQ = [
  {
    "question_group": "6.0 What is ... Second Nested level radiobutton (Radiobutton Component)?",
    "questions": [
      {
        "id": 52,
        "question": "6.0 What is ... Second Nested level radiobutton (Radiobutton Component)?",
        "answer_mlq": "One option",
        "layer_name": "cddp:local_gov_authority",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:local_gov_authority&maxFeatures=200&outputFormat=application%2Fjson",
        "expiry": "2024-12-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "lga_leg_ar",
        "operator": "LessThan",
        "value": "1950000000000",
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
    "question_group": "5.0 What is ... First level radiobutton (Radiobutton Component)?",
    "questions": [
      {
        "id": 48,
        "question": "5.0 What is ... First level radiobutton (Radiobutton Component)?",
        "answer_mlq": "Yes",
        "layer_name": "cddp:dpaw_regions",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "hectares",
        "operator": "LessThan",
        "value": "100000",
        "prefix_answer": "",
        "no_polygons_proponent": -1,
        "answer": "",
        "prefix_info": "",
        "no_polygons_assessor": -1,
        "assessor_info": "",
        "regions": "All"
      },
      {
        "id": 49,
        "question": "5.0 What is ... First level radiobutton (Radiobutton Component)?",
        "answer_mlq": "No",
        "layer_name": "cddp:dpaw_regions",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "hectares",
        "operator": "LessThan",
        "value": "10000",
        "prefix_answer": "",
        "no_polygons_proponent": -1,
        "answer": "",
        "prefix_info": "",
        "no_polygons_assessor": -1,
        "assessor_info": "",
        "regions": "All"
      },
      {
        "id": 50,
        "question": "5.0 What is ... First level radiobutton (Radiobutton Component)?",
        "answer_mlq": "Possibly",
        "layer_name": "cddp:local_gov_authority",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:local_gov_authority&maxFeatures=200&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "lga_leg_ar",
        "operator": "LessThan",
        "value": "100000",
        "prefix_answer": "",
        "no_polygons_proponent": -1,
        "answer": "",
        "prefix_info": "",
        "no_polygons_assessor": -1,
        "assessor_info": "",
        "regions": "All"
      },
      {
        "id": 51,
        "question": "5.0 What is ... First level radiobutton (Radiobutton Component)?",
        "answer_mlq": "Possibly not",
        "layer_name": "cddp:local_gov_authority",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:local_gov_authority&maxFeatures=200&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "lga_leg_ar",
        "operator": "LessThan",
        "value": "10000",
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
      "RadioTest": [
        {
          "Section5-0": "unknown"
        }
      ]
    }
  ],
  "layer_data": [
    {
      "name": "Section5-0",
      "label": "Unknown",
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
      "sqs_timestamp": "2023-03-14 11:16:17"
    }
  ],
  "add_info_assessor": {}
}

