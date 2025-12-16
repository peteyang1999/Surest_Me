USE qc_core
GO

DECLARE 
    @errMsg VARCHAR(3000),
    @errSeverity INT,
    @errState INT,
    @errLine INT,
    @errProcedure VARCHAR(128),
    @UpUser NVARCHAR(50) = 'bind\\qcprod', -- Should be dynamic if possible
    @UpDate DATETIME = GETDATE(),
    @StepName VARCHAR(100) = ''

-- STEP 1: Identify mismatched claims (CTE)
SET @StepName = 'Insert Tracking Records'

BEGIN TRY
;
;WITH cte_MismatchedClaimLines AS (
    SELECT
        c.claim_id,
        c.claim_ud,
        c.repriced_network,
        cp.claim_line_sequence,
        ark.adjudication_result_key_id,
        ark_pn.provider_network_nm AS ark_network,
        arko_pn.provider_network_nm AS arko_network,
        bnm.provider_network_nm AS expected_network,
        1 AS to_process,
        @UpUser AS created_user_name,
        @UpDate AS created_date,
        @UpDate AS modified_date,
        ROW_NUMBER() OVER (PARTITION BY ark.adjudication_result_key_id ORDER BY c.modified_date DESC) AS rn
    FROM claim c
    INNER JOIN claim_procedure cp ON c.claim_id = cp.claim_id
    INNER JOIN adjudication_result_key ark ON cp.claim_procedure_id = ark.claim_procedure_id
    LEFT JOIN provider_network ark_pn ON ark.provider_network_id = ark_pn.provider_network_id
    LEFT JOIN adjudication_result_key_override arko ON ark.adjudication_result_key_id = arko.adjudication_result_key_id
    LEFT JOIN provider_network arko_pn ON arko.provider_network_id = arko_pn.provider_network_id
    INNER JOIN dbo.bind_network_map bnm ON c.repriced_network = bnm.repriced_network
    WHERE
        c.modified_date >= '10/24/2025'--DATEADD(DAY, -1, GETDATE())
        AND cp.is_negated != '1'
        AND c.adjudication_status_id NOT IN ('4','15')
        AND NOT EXISTS (
            SELECT 1
            FROM adjudication_result_code arc WITH (NOLOCK)
            JOIN result_code rc WITH (NOLOCK) ON rc.result_code_id = arc.result_code_id
            WHERE arc.claim_id = c.claim_id
              AND rc.result_code_id IN ('1470','1838','1634')
       
  )
        -- NEW: Compare effective network (ARKO if present, else ARK) to expected
        AND COALESCE(arko_pn.provider_network_nm, ark_pn.provider_network_nm, '') 
            <> COALESCE(bnm.provider_network_nm, '')
        -- Optional: ensure at least one network exists (avoids pulling rows where both are NULL)
        AND COALESCE(arko_pn.provider_network_nm, ark_pn.provider_network_nm) IS NOT NULL
)




-- STEP 2: 
INSERT INTO [bind_mismatched_network_processing] WITH (ROWLOCK)
(
    claim_id, claim_ud, repriced_network,
    ark_network, arko_network, expected_network,
    to_process, created_user_name, created_date, modified_date, modified_user_name
)
SELECT
    claim_id, claim_ud, repriced_network,
    ark_network, arko_network, expected_network,
    to_process, created_user_name, created_date, modified_date, created_user_name
FROM cte_MismatchedClaimLines
WHERE rn = 1;

    PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' tracking records';
    
END TRY
BEGIN CATCH
    
    SELECT 
        @errMsg = 'ERROR in ' + @StepName + ': ' + ERROR_MESSAGE(),
        @errSeverity = ERROR_SEVERITY(),
        @errState = ERROR_STATE(),
        @errLine = ERROR_LINE(),
        @errProcedure = ISNULL(ERROR_PROCEDURE(), 'Insert_Tracking_Records');
    PRINT @errMsg;
    RAISERROR(@errMsg, @errSeverity, @errState);
END CATCH

-- STEP 3: Insert missing arko rows
SET @StepName = 'Insert Missing ARKO Rows'

BEGIN TRANSACTION

BEGIN TRY
    INSERT INTO adjudication_result_key_override WITH (ROWLOCK)
    (
        adjudication_result_key_id,
        provider_network_id,
        network_level_id,
        provider_network_adjudication_details_override_reason_id,
        network_level_adjudication_details_override_reason_id,
        created_user_name,
        modified_user_name,
        modified_date,
        active
    )
    
SELECT DISTINCT
    ark.adjudication_result_key_id,  -- unique per claim line
    bnm.provider_network_id,
    CASE
        WHEN bnm.provider_network_id IN (9, 10, 12) THEN 2
        WHEN bnm.provider_network_id IN (8, 11)     THEN 1
        ELSE NULL
    END AS network_level_id,
    3, 3,
    @UpUser, @UpUser, @UpDate, 1
FROM adjudication_result_key ark
INNER JOIN claim_procedure cp ON ark.claim_procedure_id = cp.claim_procedure_id
INNER JOIN claim c ON cp.claim_id = c.claim_id
INNER JOIN dbo.bind_network_map bnm ON c.repriced_network = bnm.repriced_network
LEFT JOIN adjudication_result_key_override arko ON ark.adjudication_result_key_id = arko.adjudication_result_key_id
LEFT JOIN provider_network ark_pn ON ark.provider_network_id = ark_pn.provider_network_id
WHERE
    arko.adjudication_result_key_id IS NULL
    AND c.modified_date >= '10/24/2025'--DATEADD(DAY, -1, GETDATE())
    AND cp.is_negated != '1'
    AND c.adjudication_status_id NOT IN ('4','15')
    AND NOT EXISTS (
        SELECT 1
        FROM adjudication_result_code arc WITH (NOLOCK)
        JOIN result_code rc WITH (NOLOCK) ON rc.result_code_id = arc.result_code_id
        WHERE arc.claim_id = c.claim_id
          AND rc.result_code_id IN ('1470','1838','1634')
    )
    AND ISNULL(ark_pn.provider_network_nm, '') <> ISNULL(bnm.provider_network_nm, '')
    AND NOT EXISTS (
        SELECT 1
        FROM adjudication_result_key_override t WITH (UPDLOCK, HOLDLOCK)
        WHERE t.adjudication_result_key_id = ark.adjudication_result_key_id
    );


    PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' new ARKO records';
    COMMIT TRANSACTION
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION
    SELECT 
        @errMsg = 'ERROR in ' + @StepName + ': ' + ERROR_MESSAGE(),
        @errSeverity = ERROR_SEVERITY(),
        @errState = ERROR_STATE(),
        @errLine = ERROR_LINE(),
        @errProcedure = ISNULL(ERROR_PROCEDURE(), 'Insert_Missing_ARKO');
    PRINT @errMsg;
    RAISERROR(@errMsg, @errSeverity, @errState);
END CATCH

-- STEP 4: Update existing arko rows
SET @StepName = 'Update Existing ARKO Rows'
BEGIN TRANSACTION
BEGIN TRY
    UPDATE arko WITH (ROWLOCK, UPDLOCK)
    SET 
        arko.provider_network_id = bnm.provider_network_id,
        arko.provider_network_adjudication_details_override_reason_id = 3,
        arko.network_level_id = CASE 
            WHEN bnm.provider_network_id IN (9, 10, 12) THEN 2
            WHEN bnm.provider_network_id IN (8, 11) THEN 1
            ELSE arko.network_level_id
        END,
        arko.network_level_adjudication_details_override_reason_id = 3
    FROM adjudication_result_key ark  -- flipped ark from arko here and made the ON where ark goes first, left join arko can be blank
    LEFT JOIN adjudication_result_key_override arko ON ark.adjudication_result_key_id = arko.adjudication_result_key_id
    INNER JOIN claim_procedure cp ON ark.claim_procedure_id = cp.claim_procedure_id
    INNER JOIN claim c ON cp.claim_id = c.claim_id
	LEFT JOIN adjudication_result_code arc WITH (NOLOCK) ON c.claim_id = arc.claim_id
    LEFT JOIN result_code rc WITH (NOLOCK) ON arc.result_code_id = rc.result_code_id
    INNER JOIN dbo.bind_network_map bnm ON c.repriced_network = bnm.repriced_network
    LEFT JOIN provider_network ark_pn ON ark.provider_network_id = ark_pn.provider_network_id
    INNER JOIN provider_network arko_pn ON arko.provider_network_id = arko_pn.provider_network_id -- changing this join to not allow nulls
    WHERE 
        c.modified_date >= '10/24/2025'--DATEADD(DAY, -1, GETDATE())
        AND cp.is_negated != '1'			-- = 0 or not negated
        AND c.adjudication_status_id NOT IN ('4', '15') --4 is Closed and 15 is Missing Info
		AND rc.result_code_id NOT IN ('1470', '1838', '1634') -- 1470 is BDNOELG, 1838 is BDDELETE, 1634 is BDOVRNT due to QPA network sometimes using intentional mismatch
        AND -- Prefer override, else base; compare to expected from bnm
			COALESCE(arko_pn.provider_network_nm, ark_pn.provider_network_nm, '') 
			  <> COALESCE(bnm.provider_network_nm, '')

		
		--(
            --(arko_pn.provider_network_nm IS NOT NULL AND ISNULL(arko_pn.provider_network_nm, '') != ISNULL(bnm.provider_network_nm, ''))
            --OR (arko_pn.provider_network_nm IS NULL AND ISNULL(ark_pn.provider_network_nm, '') != ISNULL(bnm.provider_network_nm, ''))
        --); -- final AND uses arko if available ark if not, Flags when it does not match bnm 

    PRINT 'Successfully updated ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' existing ARKO records';
    COMMIT TRANSACTION
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION
    SELECT 
        @errMsg = 'ERROR in ' + @StepName + ': ' + ERROR_MESSAGE(),
        @errSeverity = ERROR_SEVERITY(),
        @errState = ERROR_STATE(),
        @errLine = ERROR_LINE(),
        @errProcedure = ISNULL(ERROR_PROCEDURE(), 'Update_Existing_ARKO');
    PRINT @errMsg;
    RAISERROR(@errMsg, @errSeverity, @errState);
END CATCH

PRINT 'All steps completed successfully';