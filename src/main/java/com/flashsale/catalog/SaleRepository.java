package com.flashsale.catalog;

import java.time.Instant;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

/**
 * Database access for sales: the built-in methods (save, findById...), the live sales with a waiting
 * room (WaitingRoomAdmitter), and the newest sales (the shop's front page).
 */
public interface SaleRepository extends JpaRepository<Sale, Long> {

    @Query("select s from Sale s where s.admitPerSecond is not null and s.startsAt <= :now and s.endsAt > :now")
    List<Sale> findLiveWithWaitingRoom(Instant now);

    List<Sale> findTop20ByOrderByIdDesc();
}
