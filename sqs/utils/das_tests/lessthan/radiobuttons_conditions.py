GEOJSON = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[124.12353515624999, -30.391830328088137], [124.03564453125, -31.672083485607377], [126.69433593749999, -31.615965936476076], [127.17773437499999, -29.688052749856787], [124.12353515624999, -30.391830328088137]]]}}]}


PROPOSAL = {
  "id": 1518,
  "schema": [
  {
    "name": "proposalSummarySection",
    "type": "section",
    "label": "1. Proposal Summary",
    "children": [
      {
        "name": "Section0-7",
        "type": "radiobuttons",
        "label": "1.7 Are these planned dates",
        "options": [
          {
            "label": "Indicative",
            "value": "indicative",
            "isRequired": "True"
          },
          {
            "label": "Fixed",
            "value": "fixed"
          },
          {
            "label": "Combination",
            "value": "combination"
          }
        ],
        "conditions": {
          "fixed": [
            {
              "name": "Section0-7Group1",
              "type": "group",
              "label": "",
              "children": [
                {
                  "name": "Section0-7Group1-1",
                  "type": "radiobuttons",
                  "label": "1.7.1 Are there any implications if the proposal is delayed?",
                  "options": [
                    {
                      "label": "Yes",
                      "value": "yes",
                      "isRequired": "True"
                    },
                    {
                      "label": "No",
                      "value": "no"
                    }
                  ],
                  "conditions": {
                    "yes": [
                      {
                        "name": "Section0-7-Group1-1-YesGroup",
                        "type": "group",
                        "label": "",
                        "children": [
                          {
                            "name": "Section0-7Group1-1-Yes1",
                            "type": "text_area",
                            "label": "1.7.1.1 Outline implications of postponement",
                            "isRequired": "True"
                          },
                          {
                            "name": "Section0-7Group1-1-Yes2",
                            "type": "text_area",
                            "label": "1.7.1.2 Specify management actions to address fixed proposal date requirements",
                            "isRequired": "True",
                            "isCopiedToPermit": True,
                            "canBeEditedByAssessor": True,
                            "_help_text_assessor_url": "site_url:/help/disturbance/assessor/anchor=Section0-7Group1-1-Yes2"
                          }
                        ]
                      }
                    ]
                  },
                  "_help_text_url": "site_url:/help/disturbance/user/anchor=Section0-7Group1-1"
                }
              ]
            }
          ],
          "combination": [
            {
              "name": "Section0-7Group2",
              "type": "group",
              "label": "",
              "children": [
                {
                  "name": "Section0-7Group2-1",
                  "type": "radiobuttons",
                  "label": "1.7.2 Are there any implications if the proposal is delayed?",
                  "options": [
                    {
                      "label": "Yes",
                      "value": "yes",
                      "isRequired": "True"
                    },
                    {
                      "label": "No",
                      "value": "no"
                    }
                  ],
                  "conditions": {
                    "yes": [
                      {
                        "name": "Section0-7-Group2-1-YesGroup",
                        "type": "group",
                        "label": "",
                        "children": [
                          {
                            "name": "Section0-7Group2-1-Yes1",
                            "type": "text_area",
                            "label": "1.7.2.1 Outline implications of postponement",
                            "isRequired": "True"
                          },
                          {
                            "name": "Section0-7Group2-1-Yes2",
                            "type": "text_area",
                            "label": "1.7.2.2 Specify management actions to address fixed proposal date requirements",
                            "isRequired": "True",
                            "isCopiedToPermit": True,
                            "canBeEditedByAssessor": True,
                            "_help_text_assessor_url": "site_url:/help/disturbance/assessor/anchor=Section0-7Group2-1-Yes2"
                          }
                        ]
                      }
                    ]
                  },
                  "_help_text_url": "site_url:/help/disturbance/user/anchor=Section0-7Group2-1"
                }
              ]
            }
          ]
        },
        "help_text_url": "site_url:/help/disturbance/user/anchor=Section0-7"
      }
    ]
  }
],
  "data": []
}


MASTERLIST_QUESTIONS_GBQ = [
  {
    "question_group": "1.7.1 Are there any implications if the proposal is delayed?",
    "questions": [
      {
        "id": 52,
        "question": "1.7.1 Are there any implications if the proposal is delayed?",
        "answer_mlq": "Yes",
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
    "question_group": "1.7.1.1 Outline implications of postponement",
    "questions": [
      {
        "id": 52,
        "question": "1.7.1.1 Outline implications of postponement",
        "answer_mlq": "",
        "layer_name": "cddp:local_gov_authority",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:local_gov_authority&maxFeatures=200&outputFormat=application%2Fjson",
        "expiry": "2024-12-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "lga_leg_ar",
        "operator": "LessThan",
        "value": "1950000000000",
        "prefix_answer": "(ProponentPrefix)",
        "no_polygons_proponent": -1,
        "answer": "::lga_label",
        "prefix_info": "(AssessorPrefix)",
        "no_polygons_assessor": -1,
        "assessor_info": "::lga_label",
        "regions": "All"
      }
    ]
  },
  {
    "question_group": "1.7.1.2 Specify management actions to address fixed proposal date requirements",
    "questions": [
      {
        "id": 52,
        "question": "1.7.1.2 Specify management actions to address fixed proposal date requirements",
        "answer_mlq": "",
        "layer_name": "cddp:dpaw_regions",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "expiry": "2024-12-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "region",
        "operator": "IsNotNull",
        "value": "",
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
    "question_group": "1.7 Are these planned dates",
    "questions": [
      {
        "id": 48,
        "question": "1.7 Are these planned dates",
        "answer_mlq": "Indicative",
        "layer_name": "cddp:dpaw_regions",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "region",
        "operator": "Equals",
        "value": "Something",
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
        "question": "1.7 Are these planned dates",
        "answer_mlq": "Fixed",
        "layer_name": "cddp:dpaw_regions",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:dpaw_regions&maxFeatures=50&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "region",
        "operator": "IsNotNull",
        "value": "",
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
        "question": "1.7 Are these planned dates",
        "answer_mlq": "Combination",
        "layer_name": "cddp:local_gov_authority",
        "layer_url": "https://kmi.dbca.wa.gov.au/geoserver/cddp/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cddp:local_gov_authority&maxFeatures=200&outputFormat=application%2Fjson",
        "expiry": "2024-01-01",
        "visible_to_proponent": True,
        "buffer": 300,
        "how": "Overlapping",
        "column_name": "lga_leg_ar",
        "operator": "LessThan",
        "value": "10000000",
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
      "proposalSummarySection": [
        {
          "Section0-7": "fixed",
          "Section0-7Group1": [
            {
              "Section0-7Group1-1": "yes",
              "Section0-7-Group1-1-YesGroup": [
                {
                  "Section0-7Group1-1-Yes1": "(ProponentPrefix) SHIRE OF DUNDAS, CITY OF KALGOORLIE-BOULDER",
                  "Section0-7Group1-1-Yes2": "(ProponentPrefix) GOLDFIELDS, SOUTH COAST"
                }
              ]
            }
          ]
        }
      ]
    }
  ],
  "layer_data": [
    {
      "name": "Section0-7",
      "label": "Combination",
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
      "sqs_timestamp": "2023-03-20 14:40:38"
    },
    {
      "name": "Section0-7Group1-1",
      "label": "No",
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
      "sqs_timestamp": "2023-03-20 14:40:42"
    },
    {
      "name": "Section0-7Group1-1-Yes1",
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
      "sqs_timestamp": "2023-03-20 14:40:42"
    },
    {
      "name": "Section0-7Group1-1-Yes2",
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
      "sqs_timestamp": "2023-03-20 14:40:42"
    }
  ],
  "add_info_assessor": {
    "Section0-7": [
      []
    ],
    "Section0-7Group1-1": [
      []
    ],
    "Section0-7Group1-1-Yes1": "(AssessorPrefix) SHIRE OF MENZIES, CITY OF KALGOORLIE-BOULDER, SHIRE OF DUNDAS",
    "Section0-7Group1-1-Yes2": "(AssessorPrefix) GOLDFIELDS, SOUTH COAST"
  }
}

