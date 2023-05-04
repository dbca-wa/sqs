GEOJSON = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[124.12353515624999, -30.391830328088137], [124.03564453125, -31.672083485607377], [126.69433593749999, -31.615965936476076], [127.17773437499999, -29.688052749856787], [124.12353515624999, -30.391830328088137]]]}}]}


PROPOSAL = {
  "id": 1519,
  "schema": [
    {
      "name": "tenureSection",
      "type": "section",
      "label": "3. Tenure",
      "children": [
        {
          "name": "Section3-0",
          "type": "group",
          "label": "3.0 What is the ... land tenure (Checkbox Component)?",
          "children": [
            {
              "name": "Section3-0-1",
              "type": "checkbox",
              "group": "Section3-0",
              "label": "National park",
              "isRequired": "true"
            },
            {
              "name": "Section3-0-2",
              "type": "checkbox",
              "group": "Section3-0",
              "label": "Nature reserve"
            }
          ]
        },
        {
          "name": "Section4-0",
          "type": "group",
          "label": "4.0 What is the ... classification (Checkbox Component)?",
          "children": [
            {
              "name": "Section4-0-1",
              "type": "checkbox",
              "group": "Section4-0",
              "label": "National park",
              "isRequired": "true"
            },
            {
              "name": "Section4-0-2",
              "type": "checkbox",
              "group": "Section4-0",
              "label": "Nature reserve"
            }
          ]
        },
        {
          "name": "Section5-0",
          "type": "group",
          "label": "5.0 What is the ... some other ... classification (Checkbox Component)?",
          "children": [
            {
              "name": "Section5-0-1",
              "type": "checkbox",
              "group": "Section5-0",
              "label": "Blocks",
              "isRequired": "true"
            },
            {
              "name": "Section5-0-2",
              "type": "checkbox",
              "group": "Section5-0",
              "label": "Bricks"
            }
          ]
        }

      ]
    }
  ],
  "data": []
}


MASTERLIST_QUESTIONS_GBQ = [
  {
    "question_group": "3.0 What is the ... land tenure (Checkbox Component)?",
    "questions": [
      {
        "id": 55,
        "question": "3.0 What is the ... land tenure (Checkbox Component)?",
        "answer_mlq": "National park",
        "layer_name": "cddp:dpaw_regions",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "hectares",
        "operator": "GreateThan",
        "value": "90000000",
        "prefix_answer": "",
        "no_polygons_proponent": -1,
        "answer": "",
        "prefix_info": "",
        "no_polygons_assessor": -1,
        "assessor_info": "",
        "regions": "All"
      },
      {
        "id": 45,
        "question": "3.0 What is the ... land tenure (Checkbox Component)?",
        "answer_mlq": "Nature reserve",
        "layer_name": "cddp:dpaw_regions",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "hectares",
        "operator": "GreaterThan",
        "value": "5000000000000",
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
    "question_group": "4.0 What is the ... classification (Checkbox Component)?",
    "questions": [
      {
        "id": 46,
        "question": "4.0 What is the ... classification (Checkbox Component)?",
        "answer_mlq": "National park",
        "layer_name": "cddp:dpaw_regions",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "hectares",
        "operator": "GreaterThan",
        "value": "24000000",
        "prefix_answer": "",
        "no_polygons_proponent": -1,
        "answer": "",
        "prefix_info": "",
        "no_polygons_assessor": -1,
        "assessor_info": "",
        "regions": "All"
      },
      {
        "id": 47,
        "question": "4.0 What is the ... classification (Checkbox Component)?",
        "answer_mlq": "Nature reserve",
        "layer_name": "cddp:local_gov_authority",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:local_gov_authority&maxFeatures=200&outputFormat=application%2Fjson",
        "expiry": "2024-01-31",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "lga_leg_ar",
        "operator": "GreaterThan",
        "value": "45000000",
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
      "tenureSection": [
        {
          "Section4-0": [
            {
              "Section4-0-1": "on",
              "Section4-0-2": "on"
            }
          ]
        }
      ]
    }
  ],
  "layer_data": [
    {
      "name": "Section4-0-1",
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
      "sqs_timestamp": "2023-03-14 10:15:43"
    },
    {
      "name": "Section4-0-2",
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
      "sqs_timestamp": "2023-03-14 10:15:43"
    }
  ],
  "add_info_assessor": {}
}


