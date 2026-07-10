USE [qc_core]
GO

/****** Object:  Table [dbo].[adjudication_result_amount]    Script Date: 4/2/2026 1:51:43 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[adjudication_result_amount](
	[adjudication_result_amount_id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[claim_procedure_id] [int] NOT NULL,
	[plan_iteration_index] [int] NOT NULL,
	[authorization_required] [numeric](19, 4) NULL,
	[referral_required] [numeric](19, 4) NULL,
	[adjusted_contract_amount] [numeric](19, 4) NULL,
	[alternate_course_member_expense] [numeric](19, 4) NULL,
	[anesthesia_base_units] [numeric](19, 4) NULL,
	[authorization_cost] [numeric](19, 4) NULL,
	[authorization_units] [numeric](19, 4) NULL,
	[balance_billable] [numeric](19, 4) NULL,
	[benefit_contract_value_rate] [numeric](19, 4) NULL,
	[benefit_limit_amount] [numeric](19, 4) NULL,
	[benefit_limit_anesthesia_base_units] [numeric](19, 4) NULL,
	[benefit_limit_bna_conversion_factor] [numeric](19, 4) NULL,
	[benefit_limit_cpt_multiplier] [numeric](19, 4) NULL,
	[benefit_limit_gpci_malpractice_expense_rvu] [numeric](19, 4) NULL,
	[benefit_limit_gpci_practice_expense_rvu] [numeric](19, 4) NULL,
	[benefit_limit_gpci_work_rvu] [numeric](19, 4) NULL,
	[benefit_limit_malpractice_expense_rvu] [numeric](19, 4) NULL,
	[benefit_limit_medical_conversion_factor] [numeric](19, 4) NULL,
	[benefit_limit_practice_expense_rvu] [numeric](19, 4) NULL,
	[benefit_limit_rvu] [numeric](19, 4) NULL,
	[benefit_limit_total_anesthesia_units] [numeric](19, 4) NULL,
	[benefit_limit_work_rvu] [numeric](19, 4) NULL,
	[billed_code_contract_amount] [numeric](19, 4) NULL,
	[bna_conversion_factor] [numeric](19, 4) NULL,
	[cob_payment_excess_amount] [numeric](19, 4) NULL,
	[code_depreciation_base_amount] [numeric](19, 4) NULL,
	[code_depreciation_percent] [numeric](19, 4) NULL,
	[code_depreciation_rank] [numeric](19, 4) NULL,
	[coinsurance_amount] [numeric](19, 4) NULL,
	[coinsurance_percent] [numeric](19, 4) NULL,
	[contract_amount] [numeric](19, 4) NULL,
	[contracted_copay_amount] [numeric](19, 4) NULL,
	[copay_amount] [numeric](19, 4) NULL,
	[cpt_multiplier] [numeric](19, 4) NULL,
	[daily_limit_rank] [numeric](19, 4) NULL,
	[deductible_amount] [numeric](19, 4) NULL,
	[eligible_units] [numeric](19, 4) NULL,
	[eligible_visits] [numeric](19, 4) NULL,
	[exceeds_contract_amount] [numeric](19, 4) NULL,
	[expected_payment_amount] [numeric](19, 4) NULL,
	[expected_rvu] [numeric](19, 4) NULL,
	[externally_priced_amount] [numeric](19, 4) NULL,
	[ffs_equiv_bna_conversion_factor] [numeric](19, 4) NULL,
	[ffs_equiv_cpt_multiplier] [numeric](19, 4) NULL,
	[ffs_equiv_gpci_malpractice_expense_rvu] [numeric](19, 4) NULL,
	[ffs_equiv_gpci_practice_expense_rvu] [numeric](19, 4) NULL,
	[ffs_equiv_gpci_work_rvu] [numeric](19, 4) NULL,
	[ffs_equiv_malpractice_expense_rvu] [numeric](19, 4) NULL,
	[ffs_equiv_medical_conversion_factor] [numeric](19, 4) NULL,
	[ffs_equiv_practice_expense_rvu] [numeric](19, 4) NULL,
	[ffs_equiv_rvu] [numeric](19, 4) NULL,
	[ffs_equiv_work_rvu] [numeric](19, 4) NULL,
	[ffs_equivalent_amount] [numeric](19, 4) NULL,
	[first_coverage_distribution_amount] [numeric](19, 4) NULL,
	[first_coverage_provider_amount] [numeric](19, 4) NULL,
	[gpci_malpractice_expense_rvu] [numeric](19, 4) NULL,
	[gpci_practice_expense_rvu] [numeric](19, 4) NULL,
	[gpci_work_rvu] [numeric](19, 4) NULL,
	[indemnity_amount] [numeric](19, 4) NULL,
	[ineligible_amount] [numeric](19, 4) NULL,
	[malpractice_expense_rvu] [numeric](19, 4) NULL,
	[medical_conversion_factor] [numeric](19, 4) NULL,
	[member_expense_amount] [numeric](19, 4) NULL,
	[missing_authorization_penalty_amount] [numeric](19, 4) NULL,
	[mips_negative_adjustment_amount] [numeric](19, 4) NULL,
	[mips_positive_adjustment_amount] [numeric](19, 4) NULL,
	[sequestration_adjustment_amount] [numeric](19, 4) NULL,
	[exceeds_billed_amount] [numeric](19, 4) NULL,
	[net_payment] [numeric](19, 4) NULL,
	[pre_existing_condition_penalty_amount] [numeric](19, 4) NULL,
	[practice_expense_rvu] [numeric](19, 4) NULL,
	[provider_discount] [numeric](19, 4) NULL,
	[provider_responsibility_amount] [numeric](19, 4) NULL,
	[referral_cost] [numeric](19, 4) NULL,
	[referral_units] [numeric](19, 4) NULL,
	[rvu] [numeric](19, 4) NULL,
	[savings_amount] [numeric](19, 4) NULL,
	[supp_plan_covered_amount] [numeric](19, 4) NULL,
	[total_anesthesia_units] [numeric](19, 4) NULL,
	[total_member_responsibility] [numeric](19, 4) NULL,
	[visits] [numeric](19, 4) NULL,
	[withhold_amount] [numeric](19, 4) NULL,
	[withhold_percent] [numeric](19, 4) NULL,
	[work_rvu] [numeric](19, 4) NULL,
	[write_off_amount] [numeric](19, 4) NULL,
	[is_capitated] [bit] NULL,
	[active] [bit] NOT NULL,
	[created_user_name] [nvarchar](128) NOT NULL,
	[created_date] [datetime] NOT NULL,
	[modified_user_name] [nvarchar](128) NOT NULL,
	[modified_date] [datetime] NOT NULL,
 CONSTRAINT [pk_adjudication_result_amount] PRIMARY KEY NONCLUSTERED 
(
	[adjudication_result_amount_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[adjudication_result_amount] ADD  CONSTRAINT [DF_adjudication_result_amount_plan_iteration_index]  DEFAULT ((1)) FOR [plan_iteration_index]
GO

ALTER TABLE [dbo].[adjudication_result_amount] ADD  CONSTRAINT [DF_adjudication_result_amount_created_user_name]  DEFAULT (original_login()) FOR [created_user_name]
GO

ALTER TABLE [dbo].[adjudication_result_amount] ADD  CONSTRAINT [DF_adjudication_result_amount_created_date]  DEFAULT (getdate()) FOR [created_date]
GO

ALTER TABLE [dbo].[adjudication_result_amount] ADD  CONSTRAINT [DF_adjudication_result_amount_modified_user_name]  DEFAULT (original_login()) FOR [modified_user_name]
GO

ALTER TABLE [dbo].[adjudication_result_amount] ADD  CONSTRAINT [DF_adjudication_result_amount_modified_date]  DEFAULT (getdate()) FOR [modified_date]
GO

ALTER TABLE [dbo].[adjudication_result_amount]  WITH CHECK ADD  CONSTRAINT [fk_adjudication_result_amount_claim_procedure] FOREIGN KEY([claim_procedure_id])
REFERENCES [dbo].[claim_procedure] ([claim_procedure_id])
GO

ALTER TABLE [dbo].[adjudication_result_amount] CHECK CONSTRAINT [fk_adjudication_result_amount_claim_procedure]
GO


