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
        "operator": "LessThan",
        "value": "90000000",
        "prefix_answer": "",
        "no_polygons_proponent": -1,
        "answer": "",
        "prefix_info": "(AssessorPrefix)",
        "no_polygons_assessor": -1,
        "assessor_info": "::region",
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
        "operator": "LessThan",
        "value": "5000000000000",
        "prefix_answer": "",
        "no_polygons_proponent": -1,
        "answer": "",
        "prefix_info": "(AssessorPrefix)",
        "no_polygons_assessor": -1,
        "assessor_info": "::region",
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
        "operator": "LessThan",
        "value": "24000000",
        "prefix_answer": "",
        "no_polygons_proponent": -1,
        "answer": "",
        "prefix_info": "(AssessorPrefix)",
        "no_polygons_assessor": -1,
        "assessor_info": "::region",
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
        "operator": "LessThan",
        "value": "45000000",
        "prefix_answer": "",
        "no_polygons_proponent": -1,
        "answer": "",
        "prefix_info": "(AssessorPrefix)",
        "no_polygons_assessor": -1,
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
      "tenureSection": [
        {
          "Section3-0": [
            {
              "Section3-0-1": "on",
              "Section3-0-2": "on"
            }
          ],
          "Section4-0": [
            {
              "Section4-0-1": "on"
            }
          ]
        }
      ]
    }
  ],
  "layer_data": [
    {
      "name": "Section3-0-1",
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
      "sqs_timestamp": "2023-03-15 15:22:31"
    },
    {
      "name": "Section3-0-2",
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
      "sqs_timestamp": "2023-03-15 15:22:31"
    },
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
      "sqs_timestamp": "2023-03-15 15:22:31"
    }
  ],
  "add_info_assessor": {
    "Section3-0": [
      "(AssessorPrefix) GOLDFIELDS, SOUTH COAST"
    ],
    "Section4-0": [
      "(AssessorPrefix) GOLDFIELDS, SOUTH COAST"
    ]
  }
}
