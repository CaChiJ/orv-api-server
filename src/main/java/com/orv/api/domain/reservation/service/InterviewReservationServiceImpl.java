package com.orv.api.domain.reservation.service;

import com.orv.api.domain.reservation.repository.InterviewReservationRepository;
import com.orv.api.domain.reservation.service.dto.InterviewReservation;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class InterviewReservationServiceImpl implements InterviewReservationService {
    private final InterviewReservationRepository reservationRepository;

    @Override
    public Optional<InterviewReservation> getInterviewReservationById(UUID reservationId) {
        return reservationRepository.findInterviewReservationById(reservationId);
    }

    @Override
    public Optional<UUID> reserveInterview(UUID memberId, UUID storyboardId, OffsetDateTime reservedAt) throws Exception {
        Optional<UUID> id = reservationRepository.reserveInterview(memberId, storyboardId, reservedAt.toLocalDateTime());

        if (id.isEmpty()) {
            throw new Exception("Failed to reserve interview");
        }
        return id;
    }

    @Override
    @Transactional
    public Optional<UUID> reserveInstantInterview(UUID memberId, UUID storyboardId) throws Exception {
        OffsetDateTime scheduledAt = OffsetDateTime.now();
        // Instant interview is scheduled 9 hours later in DB logic from original code, maintaining it.
        Optional<UUID> id = reservationRepository.reserveInterview(memberId, storyboardId, scheduledAt.toLocalDateTime().plusHours(9));
        
        if (id.isEmpty()) {
             throw new Exception("Failed to reserve instant interview");
        }
        
        reservationRepository.changeInterviewReservationStatus(id.get(), "done");
        return id;
    }

    @Override
    public Optional<List<InterviewReservation>> getForwardInterviews(UUID memberId, OffsetDateTime from) {
        return reservationRepository.getReservedInterviews(memberId, from);
    }

    @Override
    public boolean markInterviewAsDone(UUID interviewId) {
        return reservationRepository.changeInterviewReservationStatus(interviewId, "done");
    }
}