package main

import (
	

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
)

// Messages
type progressMsg float64

// Model
type TuiModel struct {
	progress progress.Model
}

func NewTuiModel() TuiModel {
	return TuiModel{
		progress: progress.New(progress.WithDefaultGradient()),
	}
}

func (m TuiModel) Init() tea.Cmd {
	return nil
}

func (m TuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m, tea.Quit

	case progressMsg:
		m.progress.SetPercent(float64(msg))
		return m, nil

	default:
		return m, nil
	}
}

func (m TuiModel) View() string {
	return "\n" + m.progress.View() + "\n\nPress any key to quit"
}
