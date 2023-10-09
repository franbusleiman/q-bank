package com.busleiman.qbank.repository;

import com.busleiman.qbank.model.BankAccount;
import org.springframework.data.r2dbc.repository.R2dbcRepository;

public interface BankAccountRepository extends R2dbcRepository<BankAccount, String> {
}
