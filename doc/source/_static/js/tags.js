/**
 * Check whether a panel matches the selected filter tags.
 *
 * @param {any} panel Example gallery item
 * @param {Array<Array<string>>} groupedActiveTags Groups of tags selected by the user.
 * @returns {boolean} True if the panel should be shown, false otherwise
 */
function panelMatchesTags(panel, groupedActiveTags) {
  // Show the panel if every tagGroup has at least one active tag in the classList,
  // or if no tag in a group is selected.
  return groupedActiveTags.every(tagGroup => {
    return tagGroup.length === 0 || Array.from(panel.classList).some(tag => tagGroup.includes(tag))
  })
}

/**
 * Filter the links to the examples in the example gallery
 * by the selected tags and the current search query.
 *
 * @param {object} tags Object with grouped arrays of tags, each with className, displayName, and
 * icon
 */
function filterPanels(tags) {
  const noMatchesElement = document.querySelector("#noMatches");
  const query = document.getElementById("searchInput").value.toLowerCase();
  const panels = document.querySelectorAll('.gallery-item')
  const activeTags = Array.from(document.querySelectorAll('.tag.btn-primary')).map(el => el.id);
  const groupedActiveTags = Object.values(tags).map(tagGroup => {
    const classes = tagGroup.map(({className}) => className);
    return activeTags.filter(activeTag => classes.includes(activeTag));
  })

  // Show all panels first
  panels.forEach(panel => panel.classList.remove("hidden"));

  let toHide = [];
  let toShow = [];

  // Show each panel if it has every active tag and matches the search query
  panels.forEach(panel => {
    const text = (panel.textContent + panel.classList.toString()).toLowerCase();
    // const hasTag = activeTags.every(tag => panel.classList.contains(tag));
    const hasTag = panelMatchesTags(panel, groupedActiveTags)
    const hasText = text.includes(query.toLowerCase());

    if (hasTag && hasText) {
      toShow.push(panel);
    } else {
      toHide.push(panel);
    }
  })

  toShow.forEach(panel => panel.classList.remove("hidden"));
  toHide.forEach(panel => panel.classList.add("hidden"));

  // If no matches are found, display the noMatches element
  if (toShow.length === 0) {
      noMatchesElement.classList.remove("hidden");
    } else {
      noMatchesElement.classList.add("hidden");
  }

  // Set the URL to match the active tags using query parameters
  history.replaceState(null, null, activeTags.length === 0 ? '' : `?tags=${activeTags.join(',')}`);
}

/**
 * Generate the callback triggered when a user clicks on a tag filter button.
 * @param {string} tag The element corresponding to the tag
 * @returns {() => void} The callback that will be called when the user clicks a tag filter button
 */
function generateTagClickHandler(tag, tags) {
  return () => {
    // Toggle "tag" buttons on click.
    if (tag.classList.contains('btn-primary')) {
        // deactivate filter button
        tag.classList.replace('btn-primary', 'btn-outline-primary');
    } else {
        // activate filter button
        tag.classList.replace('btn-outline-primary', 'btn-primary');
    }
    filterPanels(tags)
  }
}

window.addEventListener('load', () => {
  document.querySelectorAll('.tag').forEach(tag => {
    tag.addEventListener('click', generateTagClickHandler(tag, tags));
  });

  const searchInput = document.getElementById("searchInput");
  if (searchInput) {
      searchInput.addEventListener("keyup", function (event) {
          event.preventDefault();
          filterPanels(tags);
      });
  }

  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.size > 0) {
      const urlTagParams = urlParams.get('tags').split(',');
      urlTagParams.forEach(tag => {
          const tagButton = document.getElementById(tag);
          if (tagButton) {
            tagButton.classList.replace('btn-outline-primary', 'btn-primary');
          }
      });
      filterPanels(tags);
  }
});
