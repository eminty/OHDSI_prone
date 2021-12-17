/*
 * #%L
 * NLP System for proning therapy
 * %%
 * Copyright (C) 2010 - 2021 Department of Veterans Affairs
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import gov.va.vinci.leo.regex.types.RegularExpressionType

/* An arbitrary name for this annotator. Used in the pipeline for the name of this annotation. */
name = "SectionHeaderAnnotator"

configuration {
    /* All configuration for this annotator. */
    defaults {
        /* Global for all configrations below if a property specified here is not overridden in a section below. */

        outputType = RegularExpressionType.class.canonicalName
        case_sensitive = false
        matchedPatternFeatureName = "pattern"
        concept_feature_name = "concept"
        groupFeatureName = "group"
    }
/*
    "separator" {
        expressions = [
                '\\r\\n *\\r\\n'
        ]
        concept_feature_value = "separator"
        //outputType = "gov.va.vinci.vent.types.SectionHeader"
    }

*/
    "pmh" {
        expressions = [
                '(past )?medica\\s+(history|hx)(:|-)',
                '\\bmhx?(:|-)',
                '\\bmh?(:|-)',
                'pohx(:|-)',
                'pmh?(:|-)',
                'past\\s*history(:|-)',
                'history(:|-)',
                'p[hm]+\\s*computerized\\s+problem\\s+list(:|-)',
                'significant\\smedical\\shx(:|-)',
                'past\\smedical\\shistory\\s?\\/\\s?problem list(:|-)',
                //SecTag Additions
                'patient_history(:|-)',
                'patient history(:|-)',
                'pt history(:|-)',
                'history/physical examination(:|-)',
                'history physical examination(:|-)',
                'clinical history(:|-)',
                'in clinical history(:|-)',
                'clinical history/indications(:|-)',
                'clinical history indication(:|-)',
                'issues briefly as following(:|-)',
                'issue briefly as following(:|-)',
                'history(:|-)',
                'current medical problems(:|-)',
                'current medical problem(:|-)',
                'history of chronic illness(:|-)',
                'history chronic illness(:|-)',
                'clinical presentation(:|-)',
                'issues briefly as follows(:|-)',
                'issue briefly as follow(:|-)',
                'interval history(:|-)',
                'past medical history and review of systems(:|-)',
                'past medical history and review of system(:|-)',
                'past medical history review system(:|-)',
                'past medical problems(:|-)',
                'past medical problem(:|-)',
                'history of past illness(:|-)',
                'history past illness(:|-)',
                'past medical history(:|-)',
                'previous medical history(:|-)',
                'hematology/oncology history(:|-)',
                'hematology oncology history(:|-)',
                'history of general health(:|-)',
                'history general health(:|-)',
                'past medical history/past surgical history(:|-)',
                'past medical history past surgical history(:|-)',
                'medical problems(:|-)',
                'medical problem(:|-)',
                'significant past medical history(:|-)',
                'history of major illnesses and injuries(:|-)',
                'history of major illness and injury(:|-)',
                'history major illness injury(:|-)',
                'past med history(:|-)',
                'past hospitalization history(:|-)',
                'medical history(:|-)',
                'past medical and surgical history(:|-)',
                'past medical surgical history(:|-)',
                'brief medical history(:|-)',
                'Past Medical History/Problem List(:|-)',
                'past medical history problem list(:|-)',
                'past medical issues(:|-)',
                'past medical issue(:|-)',
                'past_medical_history(:|-)',
                'past medical history/surgical history(:|-)',
                'past medical history surgical history(:|-)',
                'past infectious history(:|-)',
                'past medical history/family history(:|-)',
                'past medical history family history(:|-)',
                'Known Significant Medical Diagnoses and Conditions(:|-)',
                'past history(:|-)',
                'past medical history and physical examination(:|-)',
                'past medical history physical examination(:|-)',
                'past medical history/physical examination(:|-)',
                'past_medical_history_and_physical_examination(:|-)',

        ]
        concept_feature_value = "PMH"
        outputType = "gov.va.vinci.vent.types.SectionHeader"
    }

    "problem_list" {
        expressions = [
                'active problem list(:|-)',
                'chronic\\s+stable\\s+problems(:|-)',
                'PROBLEMS ?\\- ACTIVE(:|-)',
                'problem\\s*list'

        ]
        concept_feature_value = "ProblemList"
        outputType = "gov.va.vinci.vent.types.SectionHeader"
    }

    "surgery_list" {
        expressions = [
                'Surgery Position\\(s\\)(:|-)',
                'surgery\\s*position(:|-)',


        ]
        concept_feature_value = "Surgery"
        outputType = "gov.va.vinci.vent.types.SectionHeader"
    }



        "sexual and social history" {
            expressions = [
                    'SH:',
                    'social history:',
                    'PSHx:',
                    'soc\\s*hx(:|-)',
                    'mh/pshx(:|-)',
                    'sexual history',
                    'pmh[sx](:|-)',
                    'pmhx\\/pshx(:|-)',
                    'sexual history:',
                    'LGBTQ SCREENING NWI(:|-)'


            ]
            concept_feature_value = "Social_History"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }

        "HIV screening" {
            expressions = [
                    'HIV(:|-)',
                    'HIV Screening(:|-)',
                    'HIV Risk(:|-)'

            ]
            concept_feature_value = "HIV_Screening"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }
        "Observation_and_plan" {
            expressions = [
                    'ADDITIONAL ASSESSMENT(:|-)',
                    'MEDICAL\\s*DECISION\\s*MAKING/PLAN:',
                    'ASS(:|-)',
                    'ASSESMENT(:|-)',
                    'ASSESS(:|-)',
                    'ASSESSMENT(:|-)',
                    'Assessment\\s*.\\s*Plan(:|-)',
                    'CLINICAL IMPRESSION(:|-)',
                    'CLINICAL IMPRESSIONS(:|-)',
                    '\bimp(:|-)',
                    'Imp:',
                    'IMPRESSION AND RECOMMENDATION(:|-)',
                    'IMPRESSION AND RECOMMENDATIONS(:|-)',
                    'IMPRESSION RECOMMENDATION(:|-)',
                    'IMPRESSION SECTION(:|-)',
                    'IMPRESSION(:|-)',
                    'IMPRESSION/ASSESSMENT(:|-)',
                    'IMPRESSIONS(:|-)',
                    'IMPRESSIONS/ASSESSMENT(:|-)',
                    'IMPRESSSION(:|-)',
                    'IMPRESSSIONS(:|-)',
                    'INITAL IMPRESSION(:|-)',
                    'INITIAL ASSESSMENT(:|-)',
                    'INITIAL IMPRESSION(:|-)',
                    'INITIAL IMPRESSION/ASSESSMENT(:|-)',
                    'Recommendations(:|-)',
                    '\\bA/P(:|-)',
                    '\\bA/P(:|-)',
                    '\\bA(:|-)',
                    '\\r(\\n)? *ASSESSMENT AND PLAN(:|-)',
                    '\\r(\\n)? *Impression(:|-)',
                    'assessment and plan(:|-)',
                    'assessment and recommendation(:|-)',
                    'assessment and recommendations(:|-)',
                    'assessment plan(:|-)',
                    'assessment recommendation(:|-)',
                    'assessment(:|-)',
                    'assessment/plan(:|-)',
                    'assessment(:|-)',
                    'assessment_and_plan(:|-)',
                    'clinical comment(:|-)',
                    'clinical comments(:|-)',
                    'clinical impression(:|-)',
                    'diagnoses(:|-)',
                    'diagnosis(:|-)',
                    'diagnosis(:|-)',
                    'impression and plan(:|-)',
                    'impression and plans(:|-)',
                    'impression and recommendation(:|-)',
                    'impression and recommendations(:|-)',
                    'impression plan(:|-)',
                    'impression recommendation(:|-)',
                    'impression(:|-|\\*)',
                    'impresion(:|-)',
                    'impression/plan(:|-)',
                    'impression/recommendations(:|-)',
                    'initial impression(:|-)',
                    'interpretation(:|-)',
                    'objective(:|-)',
                    'plan and discussion(:|-)',
                    'plan discussion(:|-)',
                    'medical decision making(:|-)',
                    //Addednum
                    '\\bplan(:|-)',
                    'Gen(:|-)'


            ]
            concept_feature_value = "Observation_and_Plan"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }


        "medication" {
            expressions = [
                    'ACTIVE\\s*INPATIENT\\s*AND\\s*OUTPATIENT\\s*MEDICATIONS(:|-)',
                    'ACTIVE\\s*MEDICATIONS(:|-)',
                    'ACTIVE\\s*MEDICATIONS\\s*COMBINED(:|-)',
                    'ACTIVE\\s*MEDICATIONS\\s*INCLUDE(:|-)',
                    'ACTIVE\\s*MEDICATIONS\\s*LIST(:|-)',
                    'ACTIVE\\s*MEDICATIONS\\s*PRESCRIBED\\s*AT\\s*SAGINAW\\s*VAMC(:|-)',
                    'ACTIVE\\s*MEDICATIONS\\s*PRESCRIBED\\s*AT\\s*THE\\s*SAGINAW\\s*VAMC(:|-)',
                    'ACTIVE\\s*NON-VA\\s*MEDICATIONS(:|-)',
                    'ACTIVE\\s*NONVA\\s*MEDICATIONS(:|-)',
                    'ACTIVE\\s*NON\\s*VA\\s*MEDICATIONS(:|-)',
                    'ACTIVE\\s*OPT\\s*MEDICATIONS(:|-)',
                    'ACTIVE\\s*OUTPATIENT\\s*MEDICATIONS(:|-)',
                    'ACTIVE\\s*OUTPATIENT\\s*PRESCRIPTIONS(:|-)',
                    'ACTIVE\\s*VA\\s*MEDICATIONS(:|-)',
                    'ACTIVE\\s*\\s*MEDICATIONS(:|-)',
                    'ADMISSION\\s*MEDICATIONS(:|-)',
                    'ALL\\s*ACTIVE\\s*MEDICATIONS(:|-)',
                    'Active\\s*Inpatient\\s*Medications\\s*\\(including\\s*supplies\\)(:|-)',
                    'Active\\s*Inpatient\\s*Medications\\s*drug\\s*dosage(:|-)',
                    'Active\\s*Inpatient\\s*Medications\\s*status(:|-)',
                    'Active\\s*Medications\\s*\\(including\\s*supplies\\)(:|-)',
                    'Active\\s*Medications\\s*from\\s*Remote\\s*Data(:|-)',
                    'Active\\s*Outpatient\\s*Medications\\s*\\(including\\s*supplies\\)(:|-)',
                    'Active\\s*medications(:|-)',
                    'Active\\s*medications?\\s*prior\\s*to\\s*admission(:|-)',
                    'CORRECT\\s*MEDICATIONS\\s*INCLUDE(:|-)',
                    'CURRENT\\s*INPATIENT\\s*MEDICATIONS(:|-)',
                    'CURRENT\\s*INPATIENT\\s*MEDICATIONS\\s*INCLUDE(:|-)',
                    'CURRENT\\s*MEDICATIONS(:|-)',
                    'CURRENT\\s*MEDICATIONS/RECONCILIATION(:|-)',
                    'CURRENT\\s*MEDICATIONS\\s*LIST(:|-)',
                    'DISCHARGE\\s*MEDICATIONS(:|-)',
                    'DRUGS(:|-)',
                    'HEALTH\\s*SUPPLEMENTS(:|-)',
                    'HISTORY/MEDICATIONS(:|-)',
                    'HISTORY\\s*OF\\s*MEDICATION\\s*TREATMENTS(:|-)',
                    'HISTORY\\s*OF\\s*MEDICATION\\s*USE(:|-)',
                    'Home\\s*medications(:|-)',
                    'INACTIVE\\s*OUTPATIENT\\s*MEDICATIONS(:|-)',
                    'INHOSPITAL\\s*MEDICATIONS(:|-)',
                    'INPATIENT\\s*MEDICATIONS(:|-)',
                    'INPATIENT\\s*MEDICATION\\s*RECONCILIATION(:|-)',
                    'INPT\\s*MEDICATIONS(:|-)',
                    'Inpatient\\s*medications?(:|-)',
                    'Inpatient\\s*medications?\\s*=(:|-)',
                    'MEDICATION(S)\\s*REVIEW(:|-)',
                    'MEDICATIONS(:|-)',
                    'MEDICATIONS\\s*AT\\s*ADMISSION(:|-)',
                    'MEDICATIONS\\s*AT\\s*DISCHARGE(:|-)',
                    'MEDICATIONS\\s*DURING\\s*ADMISSION(:|-)',
                    'MEDICATIONS\\s*GIVEN\\s*TODAY(:|-)',
                    'MEDICATIONS\\s*ON\\s*ADMISSION(:|-)',
                    'MEDICATIONS\\s*ON\\s*DISCHARGE(:|-)',
                    'MEDICATIONS\\s*PRIOR\\s*TO\\s*ADMISSION(:|-)',
                    'MEDICATION\\s*ADMISSION(:|-)',
                    'MEDICATION\\s*AT\\s*ADMISSION(:|-)',
                    'MEDICATION\\s*AT\\s*DISCHARGE(:|-)',
                    'MEDICATION\\s*DURING\\s*ADMISSION(:|-)',
                    'MEDICATION\\s*HISTORY(:|-)',
                    'MEDICATION\\s*MANAGEMENT\\s*AT\\s*DISCHARGE(:|-)',
                    'MEDICATION\\s*ON\\s*ADMISSION(:|-)',
                    'MEDICATION\\s*PRIOR\\s*ADMISSION(:|-)',
                    'MEDICATION\\s*PRIOR\\s*TO\\s*ADMISSION(:|-)',
                    'MEDICATION\\s*RECONCILIATION(:|-)',
                    'MEDICATION\\s*RECONCILIATION\\s*REVIEW(:|-)',
                    'MEDICATION\\s*RECONCILIATION\\s*SUMMARY(:|-)',
                    'MEDICATION\\s*RECONCILLIATION(:|-)',
                    'MEDICATION\\s*REVIEW\\s*for\\s*MEDICATION\\s*RECONCILIATION(:|-)',
                    'MEDICINES\\s*AT\\s*PHARMACY(:|-)',
                    'MED\\s*RECON(:|-)',
                    'MED\\s*RECONCILIATION(:|-)',
                    'MED\\s*RECONCILIATION\\s*OUTPT(:|-)',
                    'MISUSE\\s*OF\\s*MEDICATIONS(:|-)',
                    'M\\s*E\\s*D\\s*I\\s*C\\s*A\\s*T\\s*I\\s*O\\s*N\\s*S(:|-)',
                    'NON-VA\\s*MEDICATIONS(:|-)',
                    'NON-VA\\s*PRESCRIBED(:|-)',
                    'NON-VA\\s*PRESCRIPTIONS(:|-)',
                    'NON-VA\\s*PRESCRIPTION\\s*MEDICATIONS(:|-)',
                    'NON-VA\\s*SUPPLIED\\s*MEDICATIONS(:|-)',
                    'NONVA\\s*MEDICATIONS(:|-)',
                    'NONVA\\s*MEDICATIONS\\s*LIST(:|-)',
                    'NON\\s*VA(:|-)',
                    'NON\\s*VA\\s*MEDICATIONS(:|-)',
                    'NON\\s*VA\\s*PRESCRIBED(:|-)',
                    'NON\\s*VA\\s*PRESCRIPTIONS(:|-)',
                    'NON\\s*VA\\s*PRESCRIPTION\\s*MEDICATIONS(:|-)',
                    'NON\\s*VA\\s*SUPPLIED\\s*MEDICATIONS(:|-)',
                    'Non-VA\\s*medications(:|-)',
                    'OUTPATIENT\\s*MEDICATIONS(:|-)',
                    'OUTPATIENT\\s*MEDICATION\\s*REVIEW(:|-)',
                    'OUTPT.\\s*MEDICATION\\s*RECONCILIATION(:|-)',
                    'OUTPT\\s*MEDICATIONS(:|-)',
                    'Outpatient\\s*medications(:|-)',
                    'Outpatient\\s*medications\\s*status(:|-)',
                    'Outpatient\\s*meds\\s*DRUG\\s*List(:|-)',
                    'PENDING\\s*INPATIENT\\s*MEDICATIONS(:|-)',
                    'PRE-ADMISSION\\s*MEDICATIONS(:|-)',
                    'PRE-VISIT\\s*MED\\s*RECONCILIATION(:|-)',
                    'PREADMISSION\\s*MEDICATION(:|-)',
                    'PRESENT\\s*MEDICATIONS(:|-)',
                    'PROVIDER\\s*MED\\s*RECONCILIATION(:|-)',
                    'PTA\\s*Meds(:|-)',
                    'RECONCILED\\s*MEDICATION\\s*LIST(:|-)',
                    'RECONCILIATION(:|-)',
                    'RECONCILIATION\\s*OF\\s*MEDICATIONS\\s*COMPLETED(:|-)',
                    'SIGNIFICANT\\s*MEDICATIONS(:|-)',
                    'SUBSTANCE\\s*USE/MISUSE\\s*OF\\s*MEDICATIONS(:|-)',
                    'Status\\s*Active(:|-)',
                    'VA\\s*MEDICATIONS(:|-)',
                    '\\r(\\n)? *Active Outpatient Medications(:|-)',
                    '\\r(\\n)? *meds(:|-)',
                    '\\r(\\n)?\\s*MEDICATIONS(:|-)',
                    'summary\\s*of\\s*medications'
            ]
            concept_feature_value = "Medications"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }
/*
    "Plan_Alone" {
        expressions = [
                '\\bplan:'
        ]
        concept_feature_value = "Plan"
        outputType = "gov.va.vinci.vent.types.SectionHeader"
    }
*/

        "allergy" {
            expressions = [

                    'A L L E R G I E S(:|-)',
                    'ADDITIONAL ADRS AND/OR ALLERGIES(:|-)',
                    'ADR(:|-)',
                    'ADVERSE DRUG REACTIONS(:|-)',
                    'ADVERSE EVENTS(:|-)',
                    'ADVERSE REACTION(:|-)',
                    'ADVERSE REACTIONS(:|-)',
                    'ALLERGIC DISORDER HISTORY(:|-)',
                    'ALLERGIC REACTIONS(:|-)',
                    'ALLERGIC(:|-)',
                    'ALLERGIES AND ADVERSE REACTIONS(:|-)',
                    'ALLERGIES AND SENSITIVITIES(:|-)',
                    'ALLERGIES FAMILY HISTORY(:|-)',
                    'ALLERGIES REVIEWED(:|-)',
                    'ALLERGIES TO MEDICATIONS(:|-)',
                    'ALLERGIES/ADVERSE REACTIONS(:|-)',
                    'ALLERGIES/REACTIONS(:|-)',
                    'ALLERGIES(:|-)',
                    'ALLERGY ADVERSE REACTION(:|-)',
                    'ALLERGY ENVIRONMENTAL ALLERGEN(:|-)',
                    'ALLERGY FAMILY HISTORY(:|-)',
                    'ALLERGY INFORMATION(:|-)',
                    'ALLERGY REVIEW(:|-)',
                    'ALLERGY SCREENING(:|-)',
                    'ALLERGY SYMPTOM(:|-)',
                    'ALLERGY SYMPTOMS(:|-)',
                    'ALLERGY TO ENVIRONMENTAL ALLERGEN(:|-)',
                    'ALLERGY TO ENVIRONMENTAL ALLERGENS(:|-)',
                    'ALLERGY/ADVERSE DRUG REACTION HISTORY(:|-)',
                    'ALLERGY/ADVERSE DRUG REACTION INFORMATION(:|-)',
                    'ALLERGY/ADVERSE DRUG REACTION(:|-)',
                    'ALLERGY(:|-)',
                    'CONCOMITANT MEDICATIONS(:|-)',
                    'CURRENT ALLERGIES(:|-)',
                    'DRUG ALLERGIC REACTIONS(:|-)',
                    'DRUG ALLERGIES(:|-)',
                    'DRUG SENSITIVITIES(:|-)',
                    'FOOD & DRUG REACTIONS INCLUDING ALLERGIES AS ENTERED IN CPRS(:|-)',
                    'FOOD ALLERGIES(:|-)',
                    'HISTORY ALLERGY(:|-)',
                    'HISTORY OF ALLERGIES(:|-)',
                    'KNOWN ALLERGIES(:|-)',
                    'LATEX ALLERGY(:|-)',
                    'MEDICATIONS ALLERGIES(:|-)',
                    'NEW ALLERGIES(:|-)',
                    'NEWLY IDENTIFIED ALLERGIES(:|-)',
                    'OTHER ALLERGIES(:|-)',
                    'PREVIOUSLY DOCUMENTED ALLERGIES(:|-)',
                    'SEASONAL ALLERGIES(:|-)',
                    'SEASONAL ALLERGY(:|-)',
                    'SENSITIVITIES(:|-)',
                    '\\r\\n *\\d+ *\\).? *allergies(:|-)',
                    '\\r\\n *all(:|-)',
                    '\\r\\n *allergies(:|-)',
                    '\\r\\n *allergy(:|-)',
                    'allergies/adr(:|-)',
                    'allergies(:|-)',
                    'allergy'
            ]
            concept_feature_value = "Allergies"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }
        "Chief complaint" {
            expressions = [
                    'CHIEF COMPLAINT(:|-)',
            ]
            concept_feature_value = "Complaint"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }

        "Physical Exam" {
            expressions = [
                    '\\r(\\n)? *Physical Exam\\w*?(:|-)',
                    '\\r(\\n)? *Review of systems(:|-)',
                    'PHYSICAL EXAMINATION',
                    'PE(:|-)',
                    'exam(:|-)'
            ]
            concept_feature_value = "Exam"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }


        "ED_Course" {
            expressions = [
                    'ED\\s*COURSE(:|-)',
                    'Er\\s*COURSE(:|-)',
                    'Emergency\\s*Department\\s*Course'

            ]
            concept_feature_value = "ED_Course"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }

        "labs_and_studies" {
            expressions = [
                    '\\r(\\n)? *findings *(:|-)',
                    '\\r(\\n)? *LABORATORY DATA(:|-)',
                    '\\r(\\n)? *operation and findings *(:|-)',
                    '\\r(\\n)? *operative findings *(:|-)',
                    '\\r(\\n)? *pathologic staging *(:|-)',
                    '\\r(\\n)? *pathology report *(:|-)',
                    '\\r(\\n)? *performing lab\\b *(:|-)',
                    '\\r(\\n)? *performing laboratory *(| *\r(\n)?)(:|-)',
                    '\\r(\\n)? *reporting lab *(:|-)',
                    '\\bo:',
                    'objective(:|-)',
                    's/o(:|-)',
                    'indication(:|-)',
                    'clinical indication(:|-)',
                    'indication(:|-)',
                    'indications(:|-)',
                    '\\r(\\n)? *micro *(:|-)',
                    '\\r(\\n)? *micro exam *',
                    'labs(:|-)'

            ]
            concept_feature_value = "Labs_and_Studies"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }



        "Present Illness" {
            expressions = [
                    'hpi/interval history(:|-)',
                    'hpi interval history(:|-)',
                    'patient hpi(:|-)',
                    'present illness(:|-)',
                    'history_present_illness(:|-)',
                    'history of the present illness(:|-)',
                    'history of present illness(:|-)',
                    'history present illness(:|-)',
                    'summary of present illness(:|-)',
                    'summary present illness'
            ]
            concept_feature_value = "HPI"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }

/*

/**Warning removed for being too greedy/they were orginially used
as an extra exclusion and "Other stuff" category for a project related to medications ,
as such, could be used as suggested exclusions, but may be overly specific to that project

*/
        /*
    "warnings" {
        expressions = [
                'mrn\\s*adl\\/hygiene\\s*view(:|-)',
                'negative responses(:|-)',
                'the most common effects of(:|-)',
                'to monitor(:|-)',
                '"\\s+Adverse Effects\\s*o\\s+COMMON(:|-)',
                '^common(:|-)',
                '^serious(:|-)',
                'a\\s+variety\\s+of\\s+hematologic\\s+toxicities\\s+have been reported(:|-)',
                'adverse effects include(:|-)',
                'adverse effects such as(:|-)',
                'allergic reactions,? including(:|-)',
                'bibliography(:|-)',
                'caution with colchicine(:|-)',
                'discussed\\s+medication\\s+side\\-effects(:|-)',
                'discussed\\s*(se|ade|risks?)(:|-)',
                'discussed\\s*side\\s*effects(:|-)',
                'drug.drug\\s*interactions(:|-)',
                'drug\\s*interactions(:|-)',
                'drugs that might cause(:|-)',
                'education(:|-)',
                'Education provided on(:|-)',
                'Educated on(:|-)',
                'expected toxicity?(:|-)',
                'general information(:|-)',
                'hematologic\\s*adverse reactions(:|-)',
                'informed consent(:|-)',
                'known\\s*side\\s*effects\\s*and\\s*toxicities(:|-)',
                'micromedex?(:|-)',
                'omeprazole "?warning"?(:|-)',
                'patient\\s*education(:|-)',
                'Per clinical pharmacology(:|-)',
                'per micromedix?(:|-)',
                'pinworm\\s*infections(:|-)',
                'please monitor for signs\\s*(.|and)\\s*Symptom_Terms(:|-)',
                'reactions\\s*include(:|-)',
                'reactions\\s*includeomeprazole "?warning"?(:|-)',
                'reactions\\s*reported(:|-)',
                'responses\\s*\\+\\s*HPI(:|-)',
                'responses\\s*\\-\\s*HPI(:|-)',
                'reviewed adverse reaction(:|-)',
                'serious reactions(:|-)',
                'serious\\s*adverse\\s*effects?(:|-)',
                'serious\\s*adverse\\s*effects\\s*include(:|-)',
                'severe\\s*skin\\s*reactions,?\\s*including(:|-)',
                'side\\s*effects\\s*including(:|-)',
                'Symptom_Terms\\s*of\\s*\\w+\\s*include(:|-)',
                'these include(:|-)',
                'typically(:|-)',
                'uptodate(:|-)',
                'warnings\\s*for(:|-)',
                'warnings\\s*reviewed(:|-)',
                'watch\\s*out\\s*for(:|-)',
                'watching\\s*for(:|-)',
                'were discussed(:|-)',
                'WHAT YOU SHOULD KNOW(:|-)',
                'will\\s*discuss(:|-)',
                'risks of'
        ]
        concept_feature_value = "warnings"
        outputType = "gov.va.vinci.vent.types.SectionHeader"
    }
*/


        "other" {
            expressions = [
                    '\\r(\\n)? *A signed copy of this report(:|-)',
                    '\\r(\\n)? *modified report *(:|-)',
                    '\\r(\\n)? *note *(:|-)',
                    '\\r(\\n)? *postoperative diagnosis *(| *\r(\n)?)(:|-)',
                    '\\r(\\n)? *preoperative diagnosis *(| *\r(\n)?)(:|-)',
                    '\\r(\\n)? *procedure *(:|-)',
                    '\\r(\\n)? *rectal mass *(:|-)',
                    '\\r(\\n)? *regional lymph nodes *(:|-)',
                    '\\r(\\n)? *result *(:|-)',
                    '\\r(\\n)? *smw *(:|-)',
                    '\\r(\\n)? *\\bsp\\b *(:|-)',
                    '\\r(\\n)? *submitted *(:|-)',
                    '\\r(\\n)? *summary of section *(:|-)',
                    '\\r(\\n)? *supplementary report *(:|-)',
                    '\\r(\\n)? *supplementary report\\(s\\) *(:|-)',
                    '\\r(\\n)? *synoptic report for colon rectum *(:|-)',
                    '\\r(\\n)? *test performed at *(:|-)',
                    '\\r(\\n)? *tumor synopsis *(:|-)',
                    '/es/(:|-)',
                    'medications\\s*held\\s*or\\s*discontinued\\s*upon\\s*admission(:|-)',
                    'changes/additions(:|-)',
                    'possible risks or complications include'
                    //blank/OTHER headers: Greedy regex, but occasionally necessary
                    //,'\\R{1,2} *\\w+ *: *\\R'
                   // ,'\\R{1,2} *\\w+ +\\w+ *: *\\R'
            ]
            concept_feature_value = "other"
            outputType = "gov.va.vinci.vent.types.SectionHeader"
        }

    }







