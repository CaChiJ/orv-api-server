package com.orv.api.domain.storyboard.service;

import com.orv.api.domain.storyboard.repository.StoryboardRepository;
import com.orv.api.domain.storyboard.service.dto.Scene;
import com.orv.api.domain.storyboard.service.dto.Storyboard;
import com.orv.api.domain.storyboard.service.dto.StoryboardPreviewInfo;
import com.orv.api.domain.storyboard.service.dto.Topic;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class StoryboardService {
    private final StoryboardRepository storyboardRepository;

    public Optional<Storyboard> getStoryboard(UUID storyboardId) {
        return storyboardRepository.findById(storyboardId);
    }

    public Optional<List<Scene>> getAllScenes(UUID storyboardId) {
        return storyboardRepository.findScenesByStoryboardId(storyboardId);
    }

    @Transactional
    public Optional<Scene> getSceneAndUpdateUsageHistory(UUID sceneId, UUID memberId) {
        Optional<Scene> foundScene = storyboardRepository.findSceneById(sceneId);

        if (foundScene.isEmpty()) {
            return Optional.empty();
        }

        Scene scene = foundScene.get();

        // Determine status based on scene type
        String status = scene.getSceneType().equals("END") ? "COMPLETED" : "STARTED";

        // Update usage history
        storyboardRepository.updateUsageHistory(scene.getStoryboardId(), memberId, status);

        return foundScene;
    }

    public Optional<StoryboardPreviewInfo> getStoryboardPreview(UUID storyboardId) {
        Optional<List<Scene>> scenesOrEmpty = storyboardRepository.findScenesByStoryboardId(storyboardId);

        if (scenesOrEmpty.isEmpty()) {
            return Optional.empty();
        }

        List<Scene> scenes = scenesOrEmpty.get();

        // Count questions
        long questionCount = scenes.stream()
                .filter(scene -> scene.getSceneType().equals("QUESTION"))
                .count();

        Optional<String[]> examplesOrEmpty = storyboardRepository.getStoryboardPreview(storyboardId);

        if (examplesOrEmpty.isEmpty()) {
            return Optional.empty();
        }

        String[] examples = examplesOrEmpty.get();
        List<String> exampleList = Arrays.stream(examples).toList();

        return Optional.of(new StoryboardPreviewInfo(
                storyboardId,
                (int) questionCount,
                exampleList
        ));
    }

    public Optional<List<Topic>> getTopicsOfStoryboard(UUID storyboardId) {
        return storyboardRepository.findTopicsOfStoryboard(storyboardId);
    }
}
